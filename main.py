import os
import asyncio
import logging
import math
import re
from datetime import datetime
from zoneinfo import ZoneInfo

import aiosqlite

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.errors import (
    FloodWaitError,
    PeerFloodError,
    UserPrivacyRestrictedError,
    UserNotMutualContactError,
    UserAlreadyParticipantError,
)
from telethon.tl.functions.channels import (
    InviteToChannelRequest,
    GetParticipantRequest,
)
from telethon.tl.types import Channel, Chat


# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]
ADMIN_USER_ID = int(os.environ["ADMIN_USER_ID"])

DB_PATH = "/data/manager.db"
ITALY_TZ = ZoneInfo("Europe/Rome")
PAGE_SIZE = 20

logging.basicConfig(
    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# =========================================================
# TELETHON
# =========================================================

user_client = TelegramClient(
    StringSession(TELEGRAM_SESSION),
    API_ID,
    API_HASH,
)


# =========================================================
# STATO
# =========================================================

state = {
    "group_a": None,
    "group_b": None,

    "daily_target": 1,
    "max_attempts": 3,
    "interval_minutes": 10,

    "running": False,
    "stop_requested": False,
    "telegram_locked": False,

    "waiting_for": None,

    "member_page": 0,
    "manual_ids": [],

    "extract_diag": {
        "telegram_count": 0,
        "received": 0,
        "bots": 0,
        "deleted": 0,
        "duplicates": 0,
        "saved": 0,
    },

    "last_event": "Nessuna attività",
}

worker_task = None
worker_lock = asyncio.Lock()


# =========================================================
# DATA / ORA
# =========================================================

def now_it():
    return datetime.now(ITALY_TZ)


def today_it():
    return now_it().date().isoformat()


# =========================================================
# DATABASE
# =========================================================

async def init_db():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("""
            CREATE TABLE IF NOT EXISTS processed (
                user_id INTEGER NOT NULL,
                source_id INTEGER NOT NULL,
                destination_id INTEGER NOT NULL,
                username TEXT,
                display_name TEXT,
                status TEXT NOT NULL,
                processed_at TEXT NOT NULL,
                PRIMARY KEY (
                    user_id,
                    source_id,
                    destination_id
                )
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS logs (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                created_at TEXT NOT NULL,
                level TEXT NOT NULL,
                message TEXT NOT NULL
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS settings (
                key TEXT PRIMARY KEY,
                value TEXT
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS daily_stats (
                day TEXT PRIMARY KEY,
                migrated INTEGER DEFAULT 0,
                attempts INTEGER DEFAULT 0,
                privacy INTEGER DEFAULT 0,
                already INTEGER DEFAULT 0,
                errors INTEGER DEFAULT 0,
                unconfirmed INTEGER DEFAULT 0
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS extracted_members (
                source_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL,
                username TEXT,
                display_name TEXT,
                extracted_at TEXT NOT NULL,
                PRIMARY KEY (
                    source_id,
                    user_id
                )
            )
        """)

        # Compatibilità con database precedenti
        try:
            await db.execute(
                """
                ALTER TABLE daily_stats
                ADD COLUMN unconfirmed INTEGER DEFAULT 0
                """
            )
        except Exception:
            pass

        await db.commit()


# =========================================================
# SETTINGS
# =========================================================

async def set_setting(key, value):

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO settings(key, value)
            VALUES (?, ?)
            ON CONFLICT(key)
            DO UPDATE SET value = excluded.value
            """,
            (key, str(value)),
        )

        await db.commit()


async def get_setting(key, default=None):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT value
            FROM settings
            WHERE key = ?
            """,
            (key,),
        )

        row = await cursor.fetchone()

    return row[0] if row else default


async def save_group(prefix, group):

    await set_setting(
        f"{prefix}_id",
        group["id"],
    )

    await set_setting(
        f"{prefix}_name",
        group["name"],
    )

    await set_setting(
        f"{prefix}_input",
        group["input"],
    )


async def load_settings():

    state["daily_target"] = int(
        await get_setting(
            "daily_target",
            "1",
        )
    )

    state["max_attempts"] = int(
        await get_setting(
            "max_attempts",
            "3",
        )
    )

    state["interval_minutes"] = int(
        await get_setting(
            "interval_minutes",
            "10",
        )
    )

    state["telegram_locked"] = (
        await get_setting(
            "telegram_locked",
            "0",
        ) == "1"
    )

    for prefix in (
        "group_a",
        "group_b",
    ):

        group_id = await get_setting(
            f"{prefix}_id"
        )

        name = await get_setting(
            f"{prefix}_name"
        )

        input_value = await get_setting(
            f"{prefix}_input"
        )

        if (
            group_id
            and name
            and input_value
        ):

            if str(
                input_value
            ).lstrip("-").isdigit():

                input_value = int(
                    input_value
                )

            state[prefix] = {
                "id": int(group_id),
                "name": name,
                "input": input_value,
            }


# =========================================================
# STATISTICHE
# =========================================================

async def ensure_today():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT OR IGNORE
            INTO daily_stats(day)
            VALUES (?)
            """,
            (today_it(),),
        )

        await db.commit()


async def increment_stat(
    field,
    amount=1,
):

    allowed = {
        "migrated",
        "attempts",
        "privacy",
        "already",
        "errors",
        "unconfirmed",
    }

    if field not in allowed:
        raise ValueError(
            "Campo statistico non valido"
        )

    await ensure_today()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            f"""
            UPDATE daily_stats
            SET {field} = {field} + ?
            WHERE day = ?
            """,
            (
                amount,
                today_it(),
            ),
        )

        await db.commit()


async def get_today_stats():

    await ensure_today()

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT
                migrated,
                attempts,
                privacy,
                already,
                errors,
                unconfirmed
            FROM daily_stats
            WHERE day = ?
            """,
            (today_it(),),
        )

        row = await cursor.fetchone()

    return {
        "migrated": row[0],
        "attempts": row[1],
        "privacy": row[2],
        "already": row[3],
        "errors": row[4],
        "unconfirmed": row[5],
    }


async def get_total_stats():

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute("""
            SELECT
                COALESCE(SUM(migrated), 0),
                COALESCE(SUM(attempts), 0),
                COALESCE(SUM(privacy), 0),
                COALESCE(SUM(already), 0),
                COALESCE(SUM(errors), 0),
                COALESCE(SUM(unconfirmed), 0)
            FROM daily_stats
        """)

        row = await cursor.fetchone()

    return {
        "migrated": row[0],
        "attempts": row[1],
        "privacy": row[2],
        "already": row[3],
        "errors": row[4],
        "unconfirmed": row[5],
    }


# =========================================================
# LOG
# =========================================================

async def add_log(
    message,
    level="INFO",
):

    state["last_event"] = message

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO logs(
                created_at,
                level,
                message
            )
            VALUES (?, ?, ?)
            """,
            (
                now_it().isoformat(),
                level,
                message,
            ),
        )

        await db.commit()

    logger.info(
        "%s | %s",
        level,
        message,
    )


async def clear_logs():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            "DELETE FROM logs"
        )

        await db.commit()

    state["last_event"] = (
        "Log cancellato"
    )


# =========================================================
# PROCESSATI
# =========================================================

async def save_processed(
    user,
    status,
):

    username = getattr(
        user,
        "username",
        None,
    )

    display_name = " ".join(
        value
        for value in [
            getattr(
                user,
                "first_name",
                None,
            ),
            getattr(
                user,
                "last_name",
                None,
            ),
        ]
        if value
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT OR REPLACE
            INTO processed
            (
                user_id,
                source_id,
                destination_id,
                username,
                display_name,
                status,
                processed_at
            )
            VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                user.id,
                state["group_a"]["id"],
                state["group_b"]["id"],
                username,
                display_name,
                status,
                now_it().isoformat(),
            ),
        )

        await db.commit()


async def was_processed(user_id):

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT 1
            FROM processed
            WHERE user_id = ?
              AND source_id = ?
              AND destination_id = ?
            LIMIT 1
            """,
            (
                user_id,
                state["group_a"]["id"],
                state["group_b"]["id"],
            ),
        )

        row = await cursor.fetchone()

    return row is not None


# =========================================================
# SICUREZZA BOT
# =========================================================

def is_admin(update):

    user = update.effective_user

    return (
        user is not None
        and user.id == ADMIN_USER_ID
    )


async def deny_access(update):

    if update.callback_query:

        await update.callback_query.answer(
            "⛔ Accesso non autorizzato.",
            show_alert=True,
        )

    elif update.effective_message:

        await update.effective_message.reply_text(
            "⛔ Accesso non autorizzato."
        )


# =========================================================
# GRUPPI
# =========================================================

async def resolve_group(value):

    value = value.strip()

    if "t.me/" in value:

        value = value.split(
            "t.me/",
            1,
        )[1]

        value = value.split(
            "?",
            1,
        )[0]

        value = value.strip("/")

        if value.startswith("+"):

            raise ValueError(
                "Per gruppi privati usa l'ID."
            )

        value = (
            "@"
            + value.lstrip("@")
        )

    if value.lstrip("-").isdigit():

        value = int(value)

    entity = await user_client.get_entity(
        value
    )

    if not isinstance(
        entity,
        (Channel, Chat),
    ):

        raise ValueError(
            "Non è un gruppo Telegram."
        )

    if (
        isinstance(entity, Channel)
        and not entity.megagroup
    ):

        raise ValueError(
            "È un canale, non un gruppo."
        )

    return {
        "id": entity.id,
        "name": getattr(
            entity,
            "title",
            "Gruppo",
        ),
        "input": value,
    }


# =========================================================
# VERIFICA MEMBRO IN B
# =========================================================

async def verify_in_destination(
    destination,
    user,
):

    try:

        await user_client(
            GetParticipantRequest(
                destination,
                user,
            )
        )

        return True

    except Exception:
        return False


# =========================================================
# TIMER INTERROMPIBILE
# =========================================================

async def interruptible_wait(
    minutes,
):

    total_seconds = (
        minutes * 60
    )

    for _ in range(
        total_seconds
    ):

        if (
            not state["running"]
            or state["stop_requested"]
            or state["telegram_locked"]
        ):

            return False

        await asyncio.sleep(1)

    return True


# =========================================================
# ESTRAZIONE MEMBRI A
# =========================================================

async def extract_members_a():

    if not state["group_a"]:

        raise ValueError(
            "Gruppo A non impostato"
        )

    source = await user_client.get_entity(
        state["group_a"]["input"]
    )

    me = await user_client.get_me()

    diag = {
        "telegram_count": 0,
        "received": 0,
        "bots": 0,
        "deleted": 0,
        "duplicates": 0,
        "saved": 0,
    }

    # -----------------------------------------
    # CONTEGGIO DICHIARATO DA TELEGRAM
    # -----------------------------------------

    try:

        participants = (
            await user_client.get_participants(
                source,
                limit=1,
            )
        )

        if (
            participants.total
            is not None
        ):

            diag[
                "telegram_count"
            ] = participants.total

    except Exception as e:

        logger.warning(
            "Impossibile leggere "
            "participants.total: %s",
            type(e).__name__,
        )

    # -----------------------------------------
    # ESTRAZIONE EFFETTIVA
    # -----------------------------------------

    rows = []
    seen_ids = set()

    async for user in (
        user_client.iter_participants(
            source,
            aggressive=False,
        )
    ):

        diag["received"] += 1

        # Esclude il nostro account
        if user.id == me.id:
            continue

        # Esclude bot
        if getattr(
            user,
            "bot",
            False,
        ):

            diag["bots"] += 1
            continue

        # Esclude eliminati
        if getattr(
            user,
            "deleted",
            False,
        ):

            diag["deleted"] += 1
            continue

        # Evita duplicati
        if user.id in seen_ids:

            diag["duplicates"] += 1
            continue

        seen_ids.add(
            user.id
        )

        username = getattr(
            user,
            "username",
            None,
        )

        display_name = " ".join(
            value
            for value in [
                getattr(
                    user,
                    "first_name",
                    None,
                ),
                getattr(
                    user,
                    "last_name",
                    None,
                ),
            ]
            if value
        )

        rows.append(
            (
                state["group_a"]["id"],
                user.id,
                username,
                display_name,
                now_it().isoformat(),
            )
        )

    # -----------------------------------------
    # DATABASE
    # -----------------------------------------

    async with aiosqlite.connect(
        DB_PATH
    ) as db:

        await db.execute(
            """
            DELETE FROM extracted_members
            WHERE source_id = ?
            """,
            (
                state["group_a"]["id"],
            ),
        )

        if rows:

            await db.executemany(
                """
                INSERT OR REPLACE
                INTO extracted_members
                (
                    source_id,
                    user_id,
                    username,
                    display_name,
                    extracted_at
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                rows,
            )

        await db.commit()

    diag["saved"] = len(rows)

    state["extract_diag"] = diag

    await add_log(
        "👥 ESTRAZIONE — "
        f"Telegram: "
        f"{diag['telegram_count']} | "
        f"Ricevuti: "
        f"{diag['received']} | "
        f"Salvati: "
        f"{diag['saved']}"
    )

    return diag


async def extracted_count():

    if not state["group_a"]:
        return 0

    async with aiosqlite.connect(
        DB_PATH
    ) as db:

        cursor = await db.execute(
            """
            SELECT COUNT(*)
            FROM extracted_members
            WHERE source_id = ?
            """,
            (
                state["group_a"]["id"],
            ),
        )

        row = await cursor.fetchone()

    return row[0]


async def get_members_page(page):

    offset = (
        page * PAGE_SIZE
    )

    async with aiosqlite.connect(
        DB_PATH
    ) as db:

        cursor = await db.execute(
            """
            SELECT
                user_id,
                username,
                display_name
            FROM extracted_members
            WHERE source_id = ?
            ORDER BY
                CASE
                    WHEN username IS NULL
                    THEN 1
                    ELSE 0
                END,
                username COLLATE NOCASE,
                display_name COLLATE NOCASE
            LIMIT ? OFFSET ?
            """,
            (
                state["group_a"]["id"],
                PAGE_SIZE,
                offset,
            ),
        )

        rows = await cursor.fetchall()

    return rows


async def members_page_text(page):

    total = await extracted_count()

    if total == 0:

        return (
            "👥 MEMBRI GRUPPO A\n\n"
            "Nessun membro estratto.\n\n"
            "Premi 🔄 ESTRAI/AGGIORNA."
        )

    pages = max(
        1,
        math.ceil(
            total / PAGE_SIZE
        ),
    )

    page = max(
        0,
        min(
            page,
            pages - 1,
        ),
    )

    state["member_page"] = page

    rows = await get_members_page(
        page
    )

    lines = []

    start_number = (
        page * PAGE_SIZE
        + 1
    )

    for i, row in enumerate(
        rows,
        start=start_number,
    ):

        (
            user_id,
            username,
            name,
        ) = row

        if username:

            label = (
                f"@{username}"
            )

        elif name:

            label = name

        else:

            label = (
                "Senza username"
            )

        lines.append(
            f"{i}. "
            f"{label} — "
            f"{user_id}"
        )

    return (
        "👥 MEMBRI GRUPPO A\n\n"
        f"Totale: {total}\n"
        f"Pagina: "
        f"{page + 1}/{pages}\n\n"
        + "\n".join(lines)
    )


def members_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "◀️",
                callback_data="members_prev",
            ),
            InlineKeyboardButton(
                "🔄 ESTRAI/AGGIORNA",
                callback_data="extract_members",
            ),
            InlineKeyboardButton(
                "▶️",
                callback_data="members_next",
            ),
        ],
        [
            InlineKeyboardButton(
                "➕ INVITA PER ID",
                callback_data="manual_invite",
            )
        ],
        [
            InlineKeyboardButton(
                "⬅️ INDIETRO",
                callback_data="home",
            )
        ],
    ])


# =========================================================
# INTERFACCIA
# =========================================================

def group_label(group):

    if not group:

        return "Non impostato"

    name = group["name"]

    if len(name) > 24:

        return (
            name[:21]
            + "..."
        )

    return name


def main_keyboard():

    keyboard = [

        # GRUPPI
        [
            InlineKeyboardButton(
                "📥 GRUPPO A",
                callback_data="set_a",
            ),
            InlineKeyboardButton(
                "📤 GRUPPO B",
                callback_data="set_b",
            ),
        ],

        # TARGET
        [
            InlineKeyboardButton(
                "➖",
                callback_data="target_minus",
            ),
            InlineKeyboardButton(
                f"🎯 "
                f"{state['daily_target']}"
                f"/GIORNO",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="target_plus",
            ),
        ],

        # TIMER
        [
            InlineKeyboardButton(
                "➖",
                callback_data="interval_minus",
            ),
            InlineKeyboardButton(
                f"⏱ "
                f"{state['interval_minutes']} "
                f"MIN",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="interval_plus",
            ),
        ],

        # MAX
        [
            InlineKeyboardButton(
                "➖",
                callback_data="attempts_minus",
            ),
            InlineKeyboardButton(
                f"🔎 MAX "
                f"{state['max_attempts']}",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="attempts_plus",
            ),
        ],

        # AUTO
        [
            InlineKeyboardButton(
                "🤖 AVVIA AUTOMATICO",
                callback_data="start_run",
            )
        ],

        [
            InlineKeyboardButton(
                "⏸ PAUSA",
                callback_data="pause",
            ),
            InlineKeyboardButton(
                "🛑 STOP",
                callback_data="stop",
            ),
        ],

        # MANUALE
        [
            InlineKeyboardButton(
                "👥 MEMBRI GRUPPO A",
                callback_data="members",
            ),
            InlineKeyboardButton(
                "➕ INVITA PER ID",
                callback_data="manual_invite",
            ),
        ],

        # INFO
        [
            InlineKeyboardButton(
                "📊 STATISTICHE",
                callback_data="statistics",
            ),
            InlineKeyboardButton(
                "📋 LOG",
                callback_data="logs",
            ),
        ],
    ]

    if state["telegram_locked"]:

        keyboard.append([
            InlineKeyboardButton(
                "🔓 RIABILITA INVITI",
                callback_data="unlock_confirm",
            )
        ])

    return InlineKeyboardMarkup(
        keyboard
    )


async def home_text():

    stats = await get_today_stats()

    remaining = max(
        0,
        state["daily_target"]
        - stats["migrated"],
    )

    if state["telegram_locked"]:

        status = (
            "🔒 INVITI SOSPESI"
        )

    elif state["running"]:

        status = (
            "🟢 AUTOMATICO ATTIVO"
        )

    else:

        status = "🔴 FERMO"

    return (
        "👥 BESTPRICE MEMBER MANAGER "
        "V4.3.1\n\n"

        f"📥 A: "
        f"{group_label(state['group_a'])}\n"

        f"📤 B: "
        f"{group_label(state['group_b'])}\n\n"

        "🤖 AUTOMATICO\n"

        f"🎯 Target: "
        f"{state['daily_target']}/giorno\n"

        f"⏱ Intervallo: "
        f"{state['interval_minutes']} min\n"

        f"🔎 MAX: "
        f"{state['max_attempts']}\n\n"

        "📅 OGGI\n"

        f"✅ Confermati: "
        f"{stats['migrated']}\n"

        f"🎯 Rimanenti: "
        f"{remaining}\n"

        f"🔎 Tentativi: "
        f"{stats['attempts']}\n"

        f"🛡 Privacy: "
        f"{stats['privacy']}\n"

        f"↪️ Già presenti: "
        f"{stats['already']}\n"

        f"⚠️ Non confermati: "
        f"{stats['unconfirmed']}\n"

        f"❌ Errori: "
        f"{stats['errors']}\n\n"

        f"{status}\n\n"

        f"🕐 "
        f"{now_it().strftime('%H:%M:%S')}\n"

        "Ultimo evento:\n"
        f"{state['last_event']}"
    )


# =========================================================
# INVITO SINGOLO
# =========================================================

async def invite_one(
    destination,
    user,
    mode,
):

    display = (
        f"@{user.username}"
        if getattr(
            user,
            "username",
            None,
        )
        else f"ID {user.id}"
    )

    await increment_stat(
        "attempts"
    )

    await add_log(
        f"{mode} — Tentativo: "
        f"{display}"
    )

    try:

        await user_client(
            InviteToChannelRequest(
                destination,
                [user],
            )
        )

        await asyncio.sleep(3)

        confirmed = (
            await verify_in_destination(
                destination,
                user,
            )
        )

        if confirmed:

            await increment_stat(
                "migrated"
            )

            await save_processed(
                user,
                "CONFIRMED",
            )

            await add_log(
                f"✅ {mode} — "
                f"Confermato: "
                f"{display}"
            )

            return (
                "confirmed",
                display,
            )

        await increment_stat(
            "unconfirmed"
        )

        await save_processed(
            user,
            "UNCONFIRMED",
        )

        await add_log(
            f"⚠️ {mode} — "
            f"Non confermato: "
            f"{display}",
            "WARNING",
        )

        return (
            "unconfirmed",
            display,
        )

    except UserAlreadyParticipantError:

        await increment_stat(
            "already"
        )

        await save_processed(
            user,
            "ALREADY",
        )

        await add_log(
            f"↪️ {mode} — "
            f"Già presente: "
            f"{display}"
        )

        return (
            "already",
            display,
        )

    except (
        UserPrivacyRestrictedError,
        UserNotMutualContactError,
    ):

        await increment_stat(
            "privacy"
        )

        await save_processed(
            user,
            "PRIVACY",
        )

        await add_log(
            f"🛡 {mode} — "
            f"Privacy: "
            f"{display}"
        )

        return (
            "privacy",
            display,
        )

    except FloodWaitError as e:

        state["running"] = False

        await add_log(
            f"⏳ FloodWait "
            f"{e.seconds}s. STOP.",
            "WARNING",
        )

        return (
            "flood_wait",
            display,
        )

    except PeerFloodError:

        state["running"] = False
        state["telegram_locked"] = True

        await set_setting(
            "telegram_locked",
            "1",
        )

        await add_log(
            "🔒 Telegram ha rifiutato "
            "ulteriori inviti. "
            "Automazione bloccata.",
            "WARNING",
        )

        return (
            "peer_flood",
            display,
        )

    except Exception as e:

        await increment_stat(
            "errors"
        )

        await add_log(
            f"❌ {mode} — "
            f"{display}: "
            f"{type(e).__name__}",
            "ERROR",
        )

        return (
            "error",
            display,
        )


# =========================================================
# WORKER AUTOMATICO
# =========================================================

async def migration_worker(
    application,
):

    global worker_task

    async with worker_lock:

        if (
            not state["running"]
            or state["telegram_locked"]
        ):
            return

        state["stop_requested"] = False

        attempts_cycle = 0

        await add_log(
            "▶️ Ciclo AUTO avviato"
        )

        try:

            source = (
                await user_client.get_entity(
                    state["group_a"]["input"]
                )
            )

            destination = (
                await user_client.get_entity(
                    state["group_b"]["input"]
                )
            )

            me = await user_client.get_me()

            async for user in (
                user_client.iter_participants(
                    source
                )
            ):

                if (
                    not state["running"]
                    or state["stop_requested"]
                    or state["telegram_locked"]
                ):
                    break

                stats = (
                    await get_today_stats()
                )

                if (
                    stats["migrated"]
                    >= state["daily_target"]
                ):

                    await add_log(
                        "🎯 Obiettivo "
                        "giornaliero raggiunto"
                    )

                    break

                if (
                    attempts_cycle
                    >= state["max_attempts"]
                ):

                    await add_log(
                        "🛑 MAX tentativi "
                        "raggiunto"
                    )

                    break

                if user.id == me.id:
                    continue

                if getattr(
                    user,
                    "bot",
                    False,
                ):
                    continue

                if getattr(
                    user,
                    "deleted",
                    False,
                ):
                    continue

                if await was_processed(
                    user.id
                ):
                    continue

                attempts_cycle += 1

                result, display = (
                    await invite_one(
                        destination,
                        user,
                        "🤖 AUTO",
                    )
                )

                if result in (
                    "peer_flood",
                    "flood_wait",
                ):
                    break

                stats = (
                    await get_today_stats()
                )

                if (
                    stats["migrated"]
                    >= state["daily_target"]
                ):

                    await add_log(
                        "🎯 Obiettivo "
                        "giornaliero raggiunto"
                    )

                    break

                if state["running"]:

                    await add_log(
                        "⏱ Prossima operazione "
                        f"tra "
                        f"{state['interval_minutes']} "
                        "minuti"
                    )

                    completed = (
                        await interruptible_wait(
                            state[
                                "interval_minutes"
                            ]
                        )
                    )

                    if not completed:
                        break

        except Exception as e:

            await add_log(
                f"❌ Errore AUTO: "
                f"{type(e).__name__}",
                "ERROR",
            )

            logger.exception(
                "Errore migration_worker"
            )

        finally:

            state["running"] = False

            await add_log(
                "🏁 Ciclo AUTO terminato"
            )

            worker_task = None


# =========================================================
# INVITI MANUALI
# =========================================================

async def run_manual_invites():

    ids = list(
        state["manual_ids"]
    )

    if not ids:
        return []

    destination = (
        await user_client.get_entity(
            state["group_b"]["input"]
        )
    )

    results = []

    for user_id in ids:

        if state["telegram_locked"]:
            break

        try:

            user = (
                await user_client.get_entity(
                    user_id
                )
            )

        except Exception:

            await add_log(
                "❌ 👤 MANUALE — "
                f"ID {user_id} "
                "non risolvibile",
                "ERROR",
            )

            results.append(
                f"❌ {user_id} — "
                "non trovato"
            )

            continue

        result, display = (
            await invite_one(
                destination,
                user,
                "👤 MANUALE",
            )
        )

        labels = {
            "confirmed": "✅",
            "already": "↪️",
            "privacy": "🛡",
            "unconfirmed": "⚠️",
            "peer_flood": "🔒",
            "flood_wait": "⏳",
            "error": "❌",
        }

        results.append(
            f"{labels.get(result, '❓')} "
            f"{display}"
        )

        if result in (
            "peer_flood",
            "flood_wait",
        ):
            break

    state["manual_ids"] = []

    return results


# =========================================================
# /START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not is_admin(update):

        await deny_access(update)
        return

    await update.message.reply_text(
        await home_text(),
        reply_markup=main_keyboard(),
    )


# =========================================================
# CALLBACK BUTTONS
# =========================================================

async def buttons(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    global worker_task

    if not is_admin(update):

        await deny_access(update)
        return

    query = update.callback_query

    await query.answer()

    data = query.data

    # =====================================================
    # GRUPPO A
    # =====================================================

    if data == "set_a":

        state["waiting_for"] = "a"

        await query.edit_message_text(
            "📥 IMPOSTA GRUPPO A\n\n"
            "Inserisci @username, "
            "link t.me oppure ID.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # GRUPPO B
    # =====================================================

    elif data == "set_b":

        state["waiting_for"] = "b"

        await query.edit_message_text(
            "📤 IMPOSTA GRUPPO B\n\n"
            "Inserisci @username, "
            "link t.me oppure ID.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # TARGET
    # =====================================================

    elif data == "target_minus":

        if state["daily_target"] > 1:

            state["daily_target"] -= 1

            await set_setting(
                "daily_target",
                state["daily_target"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "target_plus":

        if state["daily_target"] < 50:

            state["daily_target"] += 1

            await set_setting(
                "daily_target",
                state["daily_target"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # INTERVALLO
    # =====================================================

    elif data == "interval_minus":

        if (
            state["interval_minutes"]
            > 10
        ):

            state[
                "interval_minutes"
            ] -= 5

            await set_setting(
                "interval_minutes",
                state[
                    "interval_minutes"
                ],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "interval_plus":

        if (
            state["interval_minutes"]
            < 120
        ):

            state[
                "interval_minutes"
            ] += 5

            await set_setting(
                "interval_minutes",
                state[
                    "interval_minutes"
                ],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # MAX
    # =====================================================

    elif data == "attempts_minus":

        if state["max_attempts"] > 1:

            state["max_attempts"] -= 1

            await set_setting(
                "max_attempts",
                state["max_attempts"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "attempts_plus":

        if state["max_attempts"] < 50:

            state["max_attempts"] += 1

            await set_setting(
                "max_attempts",
                state["max_attempts"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # AVVIA AUTO
    # =====================================================

    elif data == "start_run":

        if state["telegram_locked"]:

            await query.answer(
                "🔒 Inviti sospesi.",
                show_alert=True,
            )

            return

        if state["running"]:

            await query.answer(
                "AUTO già attivo.",
                show_alert=True,
            )

            return

        if (
            not state["group_a"]
            or not state["group_b"]
        ):

            await query.answer(
                "Imposta prima A e B.",
                show_alert=True,
            )

            return

        stats = (
            await get_today_stats()
        )

        await query.edit_message_text(
            "⚠️ AVVIO AUTOMATICO\n\n"

            f"📥 "
            f"{state['group_a']['name']}\n"

            f"📤 "
            f"{state['group_b']['name']}\n\n"

            f"🎯 "
            f"{state['daily_target']}"
            f"/giorno\n"

            f"✅ Oggi: "
            f"{stats['migrated']}\n"

            f"⏱ "
            f"{state['interval_minutes']} "
            f"min\n"

            f"🔎 MAX "
            f"{state['max_attempts']}\n\n"

            "Confermi?",

            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "✅ CONFERMA",
                    callback_data="confirm_run",
                ),
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                ),
            ]]),
        )

    elif data == "confirm_run":

        if (
            state["telegram_locked"]
            or state["running"]
        ):
            return

        state["running"] = True
        state["stop_requested"] = False

        worker_task = (
            asyncio.create_task(
                migration_worker(
                    context.application
                )
            )
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # PAUSA / STOP
    # =====================================================

    elif data == "pause":

        state["running"] = False

        await add_log(
            "⏸ Pausa richiesta"
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "stop":

        state["running"] = False
        state["stop_requested"] = True

        await add_log(
            "🛑 STOP richiesto"
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # MEMBRI A
    # =====================================================

    elif data == "members":

        if not state["group_a"]:

            await query.answer(
                "Imposta prima "
                "il gruppo A.",
                show_alert=True,
            )

            return

        state["member_page"] = 0

        await query.edit_message_text(
            await members_page_text(0),
            reply_markup=members_keyboard(),
        )

    # =====================================================
    # ESTRAZIONE + DIAGNOSTICA
    # =====================================================

    elif data == "extract_members":

        if not state["group_a"]:
            return

        await query.edit_message_text(
            "⏳ Estrazione membri "
            "in corso...\n\n"
            "Sto confrontando la lista "
            "ricevuta con il numero "
            "dichiarato da Telegram."
        )

        try:

            diag = (
                await extract_members_a()
            )

            state["member_page"] = 0

            total = (
                await extracted_count()
            )

            text = (
                "🔎 DIAGNOSTICA ESTRAZIONE\n\n"

                f"👥 Telegram dichiara: "
                f"{diag['telegram_count']}\n"

                f"📥 Ricevuti dalla sessione: "
                f"{diag['received']}\n"

                f"🤖 Bot esclusi: "
                f"{diag['bots']}\n"

                f"🗑 Account eliminati: "
                f"{diag['deleted']}\n"

                f"♻️ Duplicati: "
                f"{diag['duplicates']}\n"

                f"💾 Salvati: "
                f"{diag['saved']}\n\n"

                f"📋 Disponibili nella lista: "
                f"{total}"
            )

            await query.edit_message_text(
                text,
                reply_markup=InlineKeyboardMarkup([
                    [
                        InlineKeyboardButton(
                            "👥 MOSTRA MEMBRI",
                            callback_data="members_show",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "🔄 RIPETI ESTRAZIONE",
                            callback_data="extract_members",
                        )
                    ],
                    [
                        InlineKeyboardButton(
                            "⬅️ HOME",
                            callback_data="home",
                        )
                    ],
                ]),
            )

        except Exception as e:

            await add_log(
                f"❌ Estrazione: "
                f"{type(e).__name__}",
                "ERROR",
            )

            logger.exception(
                "Errore estrazione membri"
            )

            await query.edit_message_text(
                "❌ ERRORE ESTRAZIONE\n\n"
                f"{type(e).__name__}\n\n"
                "Controlla il LOG.",
                reply_markup=InlineKeyboardMarkup([[
                    InlineKeyboardButton(
                        "⬅️ HOME",
                        callback_data="home",
                    )
                ]]),
            )

    # =====================================================
    # MOSTRA MEMBRI
    # =====================================================

    elif data == "members_show":

        state["member_page"] = 0

        await query.edit_message_text(
            await members_page_text(0),
            reply_markup=members_keyboard(),
        )

    elif data == "members_prev":

        page = max(
            0,
            state["member_page"] - 1,
        )

        await query.edit_message_text(
            await members_page_text(page),
            reply_markup=members_keyboard(),
        )

    elif data == "members_next":

        total = (
            await extracted_count()
        )

        pages = max(
            1,
            math.ceil(
                total / PAGE_SIZE
            ),
        )

        page = min(
            pages - 1,
            state["member_page"] + 1,
        )

        await query.edit_message_text(
            await members_page_text(page),
            reply_markup=members_keyboard(),
        )

    # =====================================================
    # MANUALE
    # =====================================================

    elif data == "manual_invite":

        # La preparazione della lista manuale resta disponibile anche
        # se gli inviti sono sospesi. La conferma continuerà invece
        # a rispettare telegram_locked.
        if not state["group_b"]:

            await query.answer(
                "Imposta prima "
                "il gruppo B.",
                show_alert=True,
            )

            return

        state[
            "waiting_for"
        ] = "manual_ids"

        await query.edit_message_text(
            "➕ INVITO MANUALE\n\n"

            "Incolla gli utenti da invitare.\n\n"
            "Puoi incollare direttamente una pagina di "
            "👥 MEMBRI GRUPPO A, ad esempio:\n"
            "1. @utente — 123456789\n"
            "2. @utente2 — 987654321\n\n"
            "Oppure puoi inserire semplicemente gli ID, "
            "uno per riga.\n\n"
            "Nessun invito partirà finché non premi CONFERMA.",

            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

    elif data == "manual_confirm":

        if state["telegram_locked"]:

            await query.answer(
                "🔒 Inviti sospesi.",
                show_alert=True,
            )

            return

        await query.edit_message_text(
            "⏳ Elaborazione manuale..."
        )

        results = (
            await run_manual_invites()
        )

        text = (
            "👤 RISULTATO MANUALE\n\n"
            + (
                "\n".join(results)
                if results
                else
                "Nessuna operazione "
                "eseguita."
            )
        )

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⬅️ HOME",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # STATISTICHE
    # =====================================================

    elif data == "statistics":

        today = (
            await get_today_stats()
        )

        total = (
            await get_total_stats()
        )

        text = (
            "📊 STATISTICHE\n\n"

            "📅 OGGI\n"

            f"✅ Confermati: "
            f"{today['migrated']}\n"

            f"🔎 Tentativi: "
            f"{today['attempts']}\n"

            f"🛡 Privacy: "
            f"{today['privacy']}\n"

            f"↪️ Già presenti: "
            f"{today['already']}\n"

            f"⚠️ Non confermati: "
            f"{today['unconfirmed']}\n"

            f"❌ Errori: "
            f"{today['errors']}\n\n"

            "📈 TOTALI\n"

            f"✅ Confermati: "
            f"{total['migrated']}\n"

            f"🔎 Tentativi: "
            f"{total['attempts']}\n"

            f"🛡 Privacy: "
            f"{total['privacy']}\n"

            f"↪️ Già presenti: "
            f"{total['already']}\n"

            f"⚠️ Non confermati: "
            f"{total['unconfirmed']}\n"

            f"❌ Errori: "
            f"{total['errors']}"
        )

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⬅️ INDIETRO",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # LOG
    # =====================================================

    elif data == "logs":

        async with aiosqlite.connect(
            DB_PATH
        ) as db:

            cursor = await db.execute(
                """
                SELECT
                    created_at,
                    message
                FROM logs
                ORDER BY id DESC
                LIMIT 20
                """
            )

            rows = (
                await cursor.fetchall()
            )

        lines = []

        for (
            created_at,
            message,
        ) in reversed(rows):

            try:

                dt = datetime.fromisoformat(
                    created_at
                )

                if dt.tzinfo is None:

                    dt = dt.replace(
                        tzinfo=ITALY_TZ
                    )

                else:

                    dt = dt.astimezone(
                        ITALY_TZ
                    )

                stamp = dt.strftime(
                    "%H:%M:%S"
                )

            except Exception:

                stamp = "--:--:--"

            lines.append(
                f"{stamp}  {message}"
            )

        await query.edit_message_text(
            "📋 ULTIMI EVENTI\n\n"
            + (
                "\n".join(lines)
                if lines
                else "Nessun evento."
            ),

            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔄 AGGIORNA",
                        callback_data="logs",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "🗑 PULISCI LOG",
                        callback_data="clear_logs_confirm",
                    )
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ INDIETRO",
                        callback_data="home",
                    )
                ],
            ]),
        )

    elif data == "clear_logs_confirm":

        await query.edit_message_text(
            "⚠️ Cancellare il LOG?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "🗑 SÌ",
                    callback_data="clear_logs",
                ),
                InlineKeyboardButton(
                    "❌ NO",
                    callback_data="logs",
                ),
            ]]),
        )

    elif data == "clear_logs":

        await clear_logs()

        await query.edit_message_text(
            "✅ Log cancellato.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⬅️ HOME",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # SBLOCCO
    # =====================================================

    elif data == "unlock_confirm":

        await query.edit_message_text(
            "⚠️ RIABILITARE GLI INVITI?\n\n"
            "Usalo solo dopo aver verificato "
            "che Telegram consenta nuovamente "
            "gli inviti.",

            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "🔓 CONFERMA",
                    callback_data="unlock",
                ),
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                ),
            ]]),
        )

    elif data == "unlock":

        state[
            "telegram_locked"
        ] = False

        await set_setting(
            "telegram_locked",
            "0",
        )

        await add_log(
            "🔓 Blocco inviti "
            "rimosso manualmente"
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # HOME
    # =====================================================

    elif data == "home":

        state["waiting_for"] = None
        state["manual_ids"] = []

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "noop":
        pass


# =========================================================
# INPUT TESTUALE
# =========================================================

async def text_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not is_admin(update):

        await deny_access(update)
        return

    target = state[
        "waiting_for"
    ]

    # =====================================================
    # ID MANUALI
    # =====================================================

    if target == "manual_ids":

        raw = update.message.text
        members = []
        seen_ids = set()

        # Formato della pagina MEMBRI GRUPPO A:
        # 1. @username — 123456789
        page_pattern = re.compile(
            r"^\s*\d+\.\s+(.+?)\s+[—–-]\s*(\d+)\s*$"
        )

        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue

            match = page_pattern.match(line)
            if match:
                label = match.group(1).strip()
                user_id = int(match.group(2))
                if user_id not in seen_ids:
                    seen_ids.add(user_id)
                    members.append((user_id, label))

        # Fallback: accetta anche soli ID, uno per riga/spazio/virgola.
        if not members:
            for token in re.split(r"[\s,]+", raw):
                token = token.strip()
                if token.isdigit():
                    user_id = int(token)
                    if user_id not in seen_ids:
                        seen_ids.add(user_id)
                        members.append((user_id, None))

        if not members:
            await update.message.reply_text(
                "❌ Non ho trovato utenti o ID Telegram validi."
            )
            return

        # Una pagina alla volta: massimo 20 utenti.
        members = members[:20]
        ids = [user_id for user_id, _ in members]

        state["manual_ids"] = ids
        state["waiting_for"] = None

        preview_lines = []
        for index, (user_id, label) in enumerate(members, start=1):
            if label:
                preview_lines.append(f"{index}. {label} — {user_id}")
            else:
                preview_lines.append(f"{index}. ID {user_id}")

        lock_note = (
            "\n\n🔒 Gli inviti sono attualmente sospesi: "
            "puoi preparare la lista, ma la conferma resterà bloccata "
            "finché la restrizione non sarà cessata."
            if state["telegram_locked"]
            else ""
        )

        await update.message.reply_text(
            "👤 CONFERMA INVITO MANUALE\n\n"
            f"📤 Destinazione:\n{state['group_b']['name']}\n\n"
            f"Utenti riconosciuti: {len(ids)}\n\n"
            + "\n".join(preview_lines)
            + lock_note
            + "\n\nProcedere?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "✅ CONFERMA",
                    callback_data="manual_confirm",
                ),
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                ),
            ]]),
        )

        return

    # =====================================================
    # GRUPPI
    # =====================================================

    if target not in (
        "a",
        "b",
    ):
        return

    try:

        group = await resolve_group(
            update.message.text
        )

        other = (
            state["group_b"]
            if target == "a"
            else state["group_a"]
        )

        if (
            other
            and other["id"]
            == group["id"]
        ):

            await update.message.reply_text(
                "❌ A e B devono "
                "essere diversi."
            )

            return

        if target == "a":

            state["group_a"] = group

            await save_group(
                "group_a",
                group,
            )

            label = "A"

        else:

            state["group_b"] = group

            await save_group(
                "group_b",
                group,
            )

            label = "B"

        state["waiting_for"] = None

        await add_log(
            f"⚙️ Gruppo {label} "
            f"impostato: "
            f"{group['name']}"
        )

        await update.message.reply_text(
            f"✅ GRUPPO {label} "
            f"IMPOSTATO\n\n"

            f"👥 {group['name']}\n"
            f"🆔 {group['id']}\n\n"

            "💾 Salvato.",

            reply_markup=main_keyboard(),
        )

    except Exception as e:

        logger.exception(
            "Errore impostazione gruppo"
        )

        await update.message.reply_text(
            "❌ Gruppo non utilizzabile.\n\n"
            f"Errore: "
            f"{type(e).__name__}"
        )


# =========================================================
# INIT
# =========================================================

async def post_init(
    application,
):

    await init_db()
    await load_settings()
    await ensure_today()

    await user_client.connect()

    if not (
        await user_client.is_user_authorized()
    ):

        raise RuntimeError(
            "TELEGRAM_SESSION "
            "non autorizzata."
        )

    me = await user_client.get_me()

    logger.info(
        "Account operativo: "
        "%s (%s)",
        me.first_name,
        me.id,
    )

    logger.info(
        "BestPrice Member Manager "
        "V4.3.2"
    )


async def post_shutdown(
    application,
):

    state["running"] = False
    state["stop_requested"] = True

    await user_client.disconnect()


# =========================================================
# MAIN
# =========================================================

def main():

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            buttons
        )
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT
            & ~filters.COMMAND,
            text_input,
        )
    )

    logger.info(
        "BestPrice Member Manager "
        "V4.3.2 avviato"
    )

    application.run_polling()


if __name__ == "__main__":
    main()
