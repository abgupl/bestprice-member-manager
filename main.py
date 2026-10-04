import os
import asyncio
import logging
import math
import re
import html
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
from telethon.tl.functions.contacts import GetContactsRequest
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
    "contact_interval_minutes": 5,
    "contact_queue_running": False,
    "start_time": "09:00",
    "auto_enabled": False,
    "last_autostart_day": "",

    "running": False,
    "stop_requested": False,
    "telegram_locked": False,
    "telegram_restriction_detected": False,

    "waiting_for": None,

    "member_page": 0,
    "contact_page": 0,
    "manual_ids": [],
    "manual_refs": [],

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
scheduler_task = None
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

        await db.execute("""
            CREATE TABLE IF NOT EXISTS extracted_contacts (
                user_id INTEGER PRIMARY KEY,
                username TEXT,
                display_name TEXT,
                phone TEXT,
                extracted_at TEXT NOT NULL
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

    state["max_attempts"] = 3

    state["interval_minutes"] = int(
        await get_setting(
            "interval_minutes",
            "10",
        )
    )

    state["contact_interval_minutes"] = int(
        await get_setting("contact_interval_minutes", "5")
    )

    state["start_time"] = await get_setting(
        "start_time",
        "09:00",
    )

    state["auto_enabled"] = (
        await get_setting(
            "auto_enabled",
            "0",
        ) == "1"
    )

    state["last_autostart_day"] = await get_setting(
        "last_autostart_day",
        "",
    )

    state["telegram_locked"] = (
        await get_setting(
            "telegram_locked",
            "0",
        ) == "1"
    )

    state["telegram_restriction_detected"] = (
        await get_setting(
            "telegram_restriction_detected",
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

        return True, None, None

    except Exception as e:

        return (
            False,
            type(e).__name__,
            str(e)[:220],
        )


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
# RUBRICA TELEGRAM
# =========================================================

async def extract_contacts():
    """Legge la rubrica Telegram della sessione utente e la salva localmente."""
    result = await user_client(GetContactsRequest(hash=0))
    me = await user_client.get_me()
    rows = []
    seen = set()
    for user in getattr(result, "users", []) or []:
        if user.id == me.id or getattr(user, "bot", False) or getattr(user, "deleted", False):
            continue
        if user.id in seen:
            continue
        seen.add(user.id)
        username = getattr(user, "username", None)
        display_name = " ".join(v for v in [getattr(user, "first_name", None), getattr(user, "last_name", None)] if v)
        phone = getattr(user, "phone", None)
        rows.append((user.id, username, display_name, phone, now_it().isoformat()))

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM extracted_contacts")
        if rows:
            await db.executemany("""
                INSERT OR REPLACE INTO extracted_contacts
                (user_id, username, display_name, phone, extracted_at)
                VALUES (?, ?, ?, ?, ?)
            """, rows)
        await db.commit()

    await add_log(f"📒 RUBRICA — contatti estratti: {len(rows)}")
    return len(rows)


async def contacts_count():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM extracted_contacts")
        row = await cur.fetchone()
    return int(row[0] if row else 0)


async def get_contacts_page(page):
    offset = page * PAGE_SIZE
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT user_id, username, display_name
            FROM extracted_contacts
            ORDER BY CASE WHEN display_name IS NULL OR display_name = '' THEN 1 ELSE 0 END,
                     display_name COLLATE NOCASE,
                     username COLLATE NOCASE
            LIMIT ? OFFSET ?
        """, (PAGE_SIZE, offset))
        return await cur.fetchall()


async def contacts_page_text(page):
    total = await contacts_count()
    if total == 0:
        return "📒 RUBRICA TELEGRAM\n\nNessun contatto estratto.\n\nPremi 🔄 ESTRAI/AGGIORNA."
    pages = max(1, math.ceil(total / PAGE_SIZE))
    page = max(0, min(page, pages - 1))
    state["contact_page"] = page
    rows = await get_contacts_page(page)
    lines = []
    start = page * PAGE_SIZE + 1
    for i, (user_id, username, name) in enumerate(rows, start=start):
        label = name or (f"@{username}" if username else "Senza nome")
        if username and name:
            label += f" (@{username})"
        lines.append(f"{i}. {label} — {user_id}")
    return f"📒 RUBRICA TELEGRAM\n\nTotale: {total}\nPagina: {page + 1}/{pages}\n\n" + "\n".join(lines)


def contacts_keyboard():
    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton("◀️", callback_data="contacts_prev"),
            InlineKeyboardButton("🔄 ESTRAI/AGGIORNA", callback_data="extract_contacts"),
            InlineKeyboardButton("▶️", callback_data="contacts_next"),
        ],
        [InlineKeyboardButton("➕ AGGIUNGI CONTATTI", callback_data="contacts_add")],
        [
            InlineKeyboardButton("➖", callback_data="contacts_interval_minus"),
            InlineKeyboardButton(f"⏱ {state['contact_interval_minutes']} MIN", callback_data="noop"),
            InlineKeyboardButton("➕", callback_data="contacts_interval_plus"),
        ],
        [InlineKeyboardButton("⬅️ INDIETRO", callback_data="home")],
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
        [
            InlineKeyboardButton("📥 GRUPPO A", callback_data="set_a"),
            InlineKeyboardButton("📤 GRUPPO B", callback_data="set_b"),
        ],
        [
            InlineKeyboardButton("➖", callback_data="target_minus"),
            InlineKeyboardButton(f"🎯 {state['daily_target']}/GIORNO", callback_data="noop"),
            InlineKeyboardButton("➕", callback_data="target_plus"),
        ],
        [
            InlineKeyboardButton(
                f"🕐 PARTENZA {state['start_time']}",
                callback_data="set_start_time",
            )
        ],
        [
            InlineKeyboardButton("➖", callback_data="interval_minus"),
            InlineKeyboardButton(f"⏱ {state['interval_minutes']} MIN", callback_data="noop"),
            InlineKeyboardButton("➕", callback_data="interval_plus"),
        ],
        [
            InlineKeyboardButton(
                (
                    "🛑 LIMITAZIONE TELEGRAM" if state["telegram_locked"] and state["telegram_restriction_detected"]
                    else "🔒 INVITI SOSPESI" if state["telegram_locked"]
                    else "🟢 AUTOMATICO ATTIVO" if state["running"]
                    else f"🟡 PROGRAMMATO {state['start_time']}" if state["auto_enabled"]
                    else "🤖 ATTIVA AUTOMATICO"
                ),
                callback_data=(
                    "noop" if state["telegram_locked"]
                    else "auto_status_menu" if (state["running"] or state["auto_enabled"])
                    else "start_run"
                ),
            )
        ],
        [
            InlineKeyboardButton("🛑 STOP", callback_data="stop"),
        ],
        [
            InlineKeyboardButton("👥 MEMBRI GRUPPO A", callback_data="members"),
            InlineKeyboardButton("➕ INVITA PER ID", callback_data="manual_invite"),
        ],
        [
            InlineKeyboardButton("📒 RUBRICA", callback_data="contacts"),
        ],
        [
            InlineKeyboardButton("📊 STATISTICHE", callback_data="statistics"),
            InlineKeyboardButton("📋 LOG", callback_data="logs"),
        ],
    ]

    if state["telegram_locked"]:
        keyboard.append([
            InlineKeyboardButton(
                "🔓 RIABILITA INVITI",
                callback_data="unlock_confirm",
            )
        ])

    return InlineKeyboardMarkup(keyboard)


async def home_text():

    # La HOME mostra solo le informazioni essenziali.
    # Le statistiche dettagliate restano disponibili dal pulsante STATISTICHE.
    if state["telegram_locked"] and state["telegram_restriction_detected"]:
        status = "🛑 LIMITAZIONE TELEGRAM RILEVATA — INVITI BLOCCATI"
    elif state["telegram_locked"]:
        status = "🔒 INVITI SOSPESI DAL BLOCCO LOCALE"
    elif state["running"]:
        status = "🟢 AUTOMATICO IN ESECUZIONE"
    elif state["auto_enabled"]:
        status = f"🟡 AUTOMATICO PROGRAMMATO — {state['start_time']}"
    else:
        status = "🔴 AUTOMATICO DISATTIVATO"

    return (
        "👥 BESTPRICE MEMBER MANAGER V4.5.0\n\n"
        f"📥 GRUPPO A: {group_label(state['group_a'])}\n"
        f"📤 GRUPPO B: {group_label(state['group_b'])}\n\n"
        f"{status}"
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

        await add_log(
            f"📨 {mode} — {display} — "
            "Richiesta inviata a Telegram — verifica in corso"
        )

        await add_log(
            f"🔍 {mode} — {display} — "
            "verifica presenza tra 10 secondi"
        )

        await asyncio.sleep(10)

        (
            confirmed,
            verify_error,
            verify_message,
        ) = await verify_in_destination(
            destination,
            user,
        )

        if confirmed:

            await add_log(
                f"🔍 {mode} — {display} — "
                "Verifica completata: utente presente"
            )

            await increment_stat(
                "migrated"
            )

            await save_processed(
                user,
                "CONFIRMED",
            )

            await add_log(
                f"✅ {mode} — {display} — "
                "AGGIUNTO AL GRUPPO"
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

        # La richiesta Telegram non equivale a un'aggiunta riuscita:
        # il successo viene conteggiato solo dopo la verifica effettiva.
        if verify_error == "UserNotParticipantError":
            dettaglio_verifica = (
                "Telegram ha accettato la richiesta, ma l'utente "
                "non risulta nel gruppo"
            )
        else:
            dettaglio_verifica = (
                f"Verifica non riuscita ({verify_error or 'errore sconosciuto'})"
            )

        await add_log(
            f"⚠️ {mode} — {display} — NON AGGIUNTO — "
            f"{dettaglio_verifica}",
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

    except UserPrivacyRestrictedError as e:

        await increment_stat("privacy")
        await save_processed(user, "PRIVACY")

        await add_log(
            f"🔒 {mode} — {display} — PRIVACY UTENTE — "
            "le impostazioni privacy non consentono l'aggiunta — "
            f"{type(e).__name__}: {str(e)[:220]}",
            "WARNING",
        )

        return ("privacy", display)

    except UserNotMutualContactError as e:

        await increment_stat("privacy")
        await save_processed(user, "PRIVACY")

        await add_log(
            f"🔒 {mode} — {display} — PRIVACY/CONTATTO — "
            "Telegram richiede che l'utente sia un contatto reciproco — "
            f"{type(e).__name__}: {str(e)[:220]}",
            "WARNING",
        )

        return ("privacy", display)

    except FloodWaitError as e:

        state["running"] = False

        await add_log(
            f"⏳ {mode} — {display} — PAUSA RICHIESTA DA TELEGRAM — "
            f"attesa richiesta: {e.seconds}s. Ciclo fermato; "
            "nessun blocco locale PeerFlood impostato — "
            f"{type(e).__name__}: {str(e)[:220]}",
            "WARNING",
        )

        return ("flood_wait", display)

    except PeerFloodError as e:

        state["running"] = False
        state["telegram_locked"] = True
        state["telegram_restriction_detected"] = True
        state["auto_enabled"] = False

        await set_setting("telegram_locked", "1")
        await set_setting("telegram_restriction_detected", "1")
        await set_setting("auto_enabled", "0")

        await add_log(
            f"🛑 {mode} — {display} (ID {user.id}) — LIMITAZIONE INVITI TELEGRAM — "
            f"{type(e).__name__}: {str(e)[:220]} — "
            "automatico fermato e blocco locale di sicurezza attivato. "
            "Questo errore non dimostra un problema del solo destinatario; "
            "può riguardare gli inviti anche se @SpamBot non segnala limitazioni.",
            "WARNING",
        )

        return ("peer_flood", display)

    except Exception as e:

        error_name = type(e).__name__

        # Solo errori riferiti esplicitamente al destinatario vengono salvati
        # come processati. Errori generici o del gruppo restano riprovabili.
        recipient_errors = {
            "UserIdInvalidError": "ID utente non valido",
            "InputUserDeactivatedError": "account eliminato",
            "UserDeactivatedError": "account disattivato",
            "UserDeactivatedBanError": "account disattivato da Telegram",
            "UserBlockedError": "utente bloccato",
            "UserKickedError": "utente espulso dal gruppo",
            "UserChannelsTooMuchError": "utente già in troppi gruppi/canali",
        }

        if error_name in recipient_errors:
            await increment_stat("errors")
            await save_processed(
                user,
                "CHANNEL_LIMIT" if error_name == "UserChannelsTooMuchError"
                else "RECIPIENT_ERROR",
            )
            await add_log(
                f"↪️ {mode} — {display} (ID {user.id}) — DESTINATARIO SALTATO — "
                f"{recipient_errors[error_name]} — "
                f"{error_name}: {str(e)[:220]}",
                "WARNING",
            )
            return ("error", display)

        await increment_stat("errors")

        await add_log(
            f"❌ {mode} — {display} — ERRORE TELEGRAM — "
            f"{error_name}: {str(e)[:160]}",
            "ERROR",
        )

        return (
            "error",
            display,
        )


# =========================================================
# SCHEDULER GIORNALIERO
# =========================================================

async def daily_scheduler(application):
    global worker_task

    while True:
        try:
            if (
                state["auto_enabled"]
                and not state["running"]
                and not (worker_task is not None and not worker_task.done())
                and not state["telegram_locked"]
                and state["group_a"]
                and state["group_b"]
            ):
                now = now_it()
                today = now.date().isoformat()
                current_hm = now.strftime("%H:%M")
                stats = await get_today_stats()

                if (
                    current_hm >= state["start_time"]
                    and state["last_autostart_day"] != today
                    and stats["migrated"] < state["daily_target"]
                ):
                    state["last_autostart_day"] = today
                    await set_setting("last_autostart_day", today)
                    state["running"] = True
                    state["stop_requested"] = False
                    await add_log(
                        f"⏰ Partenza automatica programmata delle {state['start_time']}"
                    )
                    worker_task = asyncio.create_task(
                        migration_worker(application)
                    )

            await asyncio.sleep(15)

        except asyncio.CancelledError:
            break
        except Exception as e:
            logger.exception("Errore daily_scheduler")
            await add_log(
                f"❌ Scheduler — {type(e).__name__}: {str(e)[:180]}",
                "ERROR",
            )
            await asyncio.sleep(30)


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

            # Solo il task proprietario può liberare lo stato globale.
            # Evita che un vecchio task azzeri il riferimento di un ciclo più recente.
            current_task = asyncio.current_task()
            if worker_task is current_task:
                state["running"] = False
                worker_task = None

            await add_log(
                "🏁 Ciclo AUTO terminato"
            )


# =========================================================
# INVITI MANUALI
# =========================================================

async def resolve_manual_user(user_id=None, username=None):
    """Risolvi un utente usando prima username/DB, poi il Gruppo A."""

    candidates = []

    if username:
        clean_username = username.strip().lstrip("@")
        if clean_username:
            candidates.append("@" + clean_username)

    # Se abbiamo un ID, recupera l'eventuale username già estratto.
    if user_id is not None and state["group_a"]:
        async with aiosqlite.connect(DB_PATH) as db:
            cursor = await db.execute(
                """
                SELECT username
                FROM extracted_members
                WHERE source_id = ? AND user_id = ?
                LIMIT 1
                """,
                (state["group_a"]["id"], int(user_id)),
            )
            row = await cursor.fetchone()

        if row and row[0]:
            candidate = "@" + row[0].lstrip("@")
            if candidate not in candidates:
                candidates.append(candidate)

    # Username è il metodo più affidabile quando disponibile.
    for candidate in candidates:
        try:
            return await user_client.get_entity(candidate)
        except Exception:
            pass

    # Prova l'ID dalla cache/sessione Telethon.
    if user_id is not None:
        try:
            return await user_client.get_entity(int(user_id))
        except Exception:
            pass

    # Se l'ID arriva dalla rubrica estratta, recupera l'eventuale username
    # salvato. Questo rende affidabile l'incolla delle pagine RUBRICA anche
    # dopo un riavvio del bot, senza dipendere solo dalla cache Telethon.
    if user_id is not None:
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    "SELECT username FROM extracted_contacts WHERE user_id = ? LIMIT 1",
                    (int(user_id),),
                )
                row = await cursor.fetchone()
            if row and row[0]:
                try:
                    return await user_client.get_entity("@" + row[0].lstrip("@"))
                except Exception:
                    pass
        except Exception:
            pass

    # Ultimo fallback: cerca l'ID nella lista del Gruppo A accessibile
    # alla sessione. Questo evita di dipendere dalla cache dopo un restart.
    if user_id is not None and state["group_a"]:
        try:
            source = await user_client.get_entity(state["group_a"]["input"])
            async for participant in user_client.iter_participants(source):
                if participant.id == int(user_id):
                    return participant
        except Exception:
            pass

    return None


async def run_manual_invites():

    refs = list(state.get("manual_refs") or [])

    # Compatibilità con eventuali liste preparate dalla versione precedente.
    if not refs:
        refs = [
            {"id": user_id, "username": None, "label": None}
            for user_id in state["manual_ids"]
        ]

    if not refs:
        return []

    destination = await user_client.get_entity(
        state["group_b"]["input"]
    )

    results = []

    for ref in refs:

        if state["telegram_locked"]:
            break

        user_id = ref.get("id")
        username = ref.get("username")
        shown = (
            "@" + username.lstrip("@")
            if username
            else (f"ID {user_id}" if user_id is not None else "utente")
        )

        user = await resolve_manual_user(
            user_id=user_id,
            username=username,
        )

        if user is None:
            await add_log(
                f"❌ 👤 MANUALE — {shown} non risolvibile",
                "ERROR",
            )
            results.append(f"❌ {shown} — non trovato")
            continue

        result, display = await invite_one(
            destination,
            user,
            "👤 MANUALE",
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
            f"{labels.get(result, '❓')} {display}"
        )

        if result in ("peer_flood", "flood_wait"):
            break

    state["manual_ids"] = []
    state["manual_refs"] = []

    return results


async def run_contact_queue(bot, chat_id, refs):
    """Aggiunge il blocco Rubrica in background rispettando il timing dedicato."""
    try:
        destination = await user_client.get_entity(state["group_b"]["input"])
        total = len(refs)
        for index, ref in enumerate(refs, start=1):
            if state["telegram_locked"]:
                await add_log("🛑 📒 RUBRICA — coda fermata: inviti sospesi")
                break
            user = await resolve_manual_user(user_id=ref.get("id"), username=ref.get("username"))
            shown = ("@" + ref["username"].lstrip("@")) if ref.get("username") else f"ID {ref.get('id')}"
            if user is None:
                await add_log(f"❌ 📒 RUBRICA — {shown} non risolvibile", "ERROR")
            else:
                result, display = await invite_one(destination, user, "📒 RUBRICA")
                if result in ("peer_flood", "flood_wait"):
                    break
            if index < total and not state["telegram_locked"]:
                minutes = state["contact_interval_minutes"]
                await add_log(f"⏱ 📒 RUBRICA — prossimo contatto tra {minutes} minuti")
                await asyncio.sleep(minutes * 60)
        if state["telegram_locked"]:
            await bot.send_message(chat_id, "🔒 Coda Rubrica interrotta per una limitazione Telegram. Controlla il LOG.")
        else:
            await bot.send_message(chat_id, "✅ Coda Rubrica completata. Controlla il LOG per il dettaglio.")
    except Exception as e:
        await add_log(f"❌ 📒 RUBRICA — errore coda: {type(e).__name__}: {e}", "ERROR")
        try:
            await bot.send_message(chat_id, f"❌ Errore nella coda Rubrica: {type(e).__name__}")
        except Exception:
            pass
    finally:
        state["contact_queue_running"] = False
        state["manual_ids"] = []
        state["manual_refs"] = []


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
    # ORARIO PARTENZA
    # =====================================================

    elif data == "set_start_time":

        state["waiting_for"] = "start_time"
        await query.edit_message_text(
            "🕐 ORARIO PARTENZA\n\n"
            "Scrivi l'orario giornaliero nel formato HH:MM.\n"
            "Esempio: 09:00",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ ANNULLA", callback_data="home")
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

            f"🕐 Partenza giornaliera: {state['start_time']}\n"
            f"⏱ {state['interval_minutes']} min\n\n"
            "L'automatico resterà programmato ogni giorno finché non premi STOP.\n\n"
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
            or (worker_task is not None and not worker_task.done())
        ):
            await query.answer(
                "🟢 Un ciclo automatico è già in esecuzione." if not state["telegram_locked"] else "🔒 Inviti sospesi.",
                show_alert=True,
            )
            return

        state["auto_enabled"] = True
        state["stop_requested"] = False
        await set_setting("auto_enabled", "1")

        stats = await get_today_stats()
        now = now_it()
        today = now.date().isoformat()

        if (
            stats["migrated"] < state["daily_target"]
            and now.strftime("%H:%M") >= state["start_time"]
        ):
            state["last_autostart_day"] = today
            await set_setting("last_autostart_day", today)
            state["running"] = True
            worker_task = asyncio.create_task(
                migration_worker(context.application)
            )
            await add_log("🤖 Automatico giornaliero attivato — avvio immediato")
        else:
            await add_log(
                f"🌙 Automatico giornaliero attivato — partenza {state['start_time']}"
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # PANNELLO STATO AUTOMATICO
    # =====================================================

    elif data == "auto_status_menu":

        if state["telegram_locked"]:
            await query.answer(
                "🔒 Inviti sospesi.",
                show_alert=True,
            )
            return

        if state["running"]:
            stato = "🟢 AUTOMATICO ATTIVO\n\nIl ciclo è in esecuzione in questo momento."
            keyboard = [
                [InlineKeyboardButton("🛑 FERMA AUTOMATICO", callback_data="stop")],
                [InlineKeyboardButton("⬅️ INDIETRO", callback_data="home")],
            ]
        else:
            stato = (
                f"🟡 AUTOMATICO PROGRAMMATO\n\n"
                f"🕐 Partenza giornaliera: {state['start_time']}\n"
                f"🎯 Target: {state['daily_target']}/giorno\n"
                f"⏱ Intervallo: {state['interval_minutes']} min\n\n"
                "Puoi lasciarlo programmato oppure avviarlo subito."
            )
            keyboard = [
                [InlineKeyboardButton("▶️ AVVIA ORA", callback_data="start_now")],
                [InlineKeyboardButton("🕐 CAMBIA ORARIO", callback_data="set_start_time")],
                [InlineKeyboardButton("⛔ DISATTIVA AUTOMATICO", callback_data="stop")],
                [InlineKeyboardButton("⬅️ INDIETRO", callback_data="home")],
            ]

        await query.edit_message_text(
            stato,
            reply_markup=InlineKeyboardMarkup(keyboard),
        )

    # =====================================================
    # AVVIA SUBITO IL CICLO PROGRAMMATO
    # =====================================================

    elif data == "start_now":

        if state["telegram_locked"]:
            await query.answer("🔒 Inviti sospesi.", show_alert=True)
            return

        if state["running"] or (worker_task is not None and not worker_task.done()):
            await query.answer(
                "🟢 Automatico già in esecuzione. Non è stato avviato un secondo ciclo.",
                show_alert=True,
            )
            return

        if not state["group_a"] or not state["group_b"]:
            await query.answer("Imposta prima A e B.", show_alert=True)
            return

        stats = await get_today_stats()
        if stats["migrated"] >= state["daily_target"]:
            await query.answer(
                "🎯 Target giornaliero già raggiunto.",
                show_alert=True,
            )
            return

        state["auto_enabled"] = True
        state["stop_requested"] = False
        state["running"] = True
        await set_setting("auto_enabled", "1")

        today = now_it().date().isoformat()
        state["last_autostart_day"] = today
        await set_setting("last_autostart_day", today)

        await add_log("▶️ Automatico avviato manualmente dal pannello PROGRAMMATO")
        worker_task = asyncio.create_task(
            migration_worker(context.application)
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # STOP
    # =====================================================

    elif data == "stop":

        state["running"] = False
        state["stop_requested"] = True
        state["auto_enabled"] = False
        await set_setting("auto_enabled", "0")

        await add_log(
            "🛑 Automatico disattivato manualmente"
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
    # RUBRICA TELEGRAM
    # =====================================================

    elif data == "contacts":
        state["contact_page"] = 0
        await query.edit_message_text(
            await contacts_page_text(0),
            reply_markup=contacts_keyboard(),
        )

    elif data == "extract_contacts":
        await query.edit_message_text("⏳ Lettura rubrica Telegram in corso...")
        try:
            total = await extract_contacts()
            state["contact_page"] = 0
            await query.edit_message_text(
                f"✅ RUBRICA AGGIORNATA\n\nContatti trovati: {total}\n\n"
                "La lista serve per preparare gli inviti dei contatti che hanno già dato il consenso.",
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("📒 MOSTRA RUBRICA", callback_data="contacts")],
                    [InlineKeyboardButton("⬅️ HOME", callback_data="home")],
                ]),
            )
        except Exception as e:
            await add_log(f"❌ RUBRICA — {type(e).__name__}: {e}", "ERROR")
            await query.edit_message_text(
                f"❌ Impossibile leggere la rubrica.\n\n{type(e).__name__}",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ HOME", callback_data="home")]]),
            )

    elif data == "contacts_prev":
        page = max(0, state["contact_page"] - 1)
        await query.edit_message_text(await contacts_page_text(page), reply_markup=contacts_keyboard())

    elif data == "contacts_next":
        total = await contacts_count()
        pages = max(1, math.ceil(total / PAGE_SIZE))
        page = min(pages - 1, state["contact_page"] + 1)
        await query.edit_message_text(await contacts_page_text(page), reply_markup=contacts_keyboard())

    elif data == "contacts_add":
        if not state["group_b"]:
            await query.answer("Imposta prima il gruppo B.", show_alert=True)
            return

        # La rubrica viene usata come sorgente manuale: l'admin copia una
        # pagina da 20 contatti e la incolla qui. Nessun contatto dell'intera
        # rubrica viene messo automaticamente in coda.
        state["waiting_for"] = "contact_ids"

        await query.edit_message_text(
            "➕ AGGIUNGI CONTATTI RUBRICA\n\n"
            "Incolla qui un blocco della 📒 RUBRICA TELEGRAM.\n\n"
            "Puoi incollare direttamente la pagina da 20 contatti, ad esempio:\n"
            "1. Mario Rossi (@mario) — 123456789\n"
            "2. @luca — 987654321\n\n"
            "Il bot riconoscerà al massimo 20 contatti per volta e, dopo la tua "
            "conferma, proverà ad aggiungerli direttamente al Gruppo B.\n\n"
            "Usa questa funzione solo per contatti che hanno già dato il consenso.\n"
            "Nessuna operazione partirà finché non premi CONFERMA.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("❌ ANNULLA", callback_data="contacts")
            ]]),
        )

    elif data == "contacts_interval_minus":
        if state["contact_interval_minutes"] > 1:
            state["contact_interval_minutes"] -= 1
            await set_setting("contact_interval_minutes", state["contact_interval_minutes"])
        await query.edit_message_text(await contacts_page_text(state["contact_page"]), reply_markup=contacts_keyboard())

    elif data == "contacts_interval_plus":
        if state["contact_interval_minutes"] < 120:
            state["contact_interval_minutes"] += 1
            await set_setting("contact_interval_minutes", state["contact_interval_minutes"])
        await query.edit_message_text(await contacts_page_text(state["contact_page"]), reply_markup=contacts_keyboard())

    elif data == "contacts_confirm":
        if state["telegram_locked"]:
            await query.answer("🔒 Inviti sospesi.", show_alert=True)
            return
        if state.get("contact_queue_running"):
            await query.answer("📒 Una coda Rubrica è già in esecuzione.", show_alert=True)
            return
        refs = list(state.get("manual_refs") or [])
        if not refs:
            await query.answer("Nessun contatto preparato.", show_alert=True)
            return
        state["contact_queue_running"] = True
        asyncio.create_task(run_contact_queue(context.bot, query.message.chat_id, refs))
        await query.edit_message_text(
            f"▶️ CODA RUBRICA AVVIATA\n\nContatti: {len(refs)}\n"
            f"⏱ Intervallo: {state['contact_interval_minutes']} minuti\n\n"
            "Puoi continuare a usare il bot: la coda procede in background.",
            reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("⬅️ HOME", callback_data="home")]])
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
            "👥 MEMBRI GRUPPO A oppure 📒 RUBRICA TELEGRAM, ad esempio:\n"
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
            "Il blocco locale è stato attivato perché Telegram ha segnalato una limitazione.\n\n"
            "Sblocca solo dopo aver verificato (ad esempio con @SpamBot) "
            "che l'account non sia più limitato. Lo sblocco del bot non rimuove "
            "una limitazione applicata da Telegram.",

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
        state["telegram_restriction_detected"] = False

        await set_setting(
            "telegram_locked",
            "0",
        )
        await set_setting(
            "telegram_restriction_detected",
            "0",
        )

        await add_log(
            "🔓 Blocco locale rimosso manualmente — "
            "limitazione Telegram marcata come verificata/risolta dall'amministratore"
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
        state["manual_refs"] = []

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
    # ORARIO PARTENZA
    # =====================================================

    if target == "start_time":
        value = update.message.text.strip()
        match = re.fullmatch(r"([01]\d|2[0-3]):([0-5]\d)", value)

        if not match:
            await update.message.reply_text(
                "❌ Orario non valido. Usa HH:MM, ad esempio 09:00."
            )
            return

        state["start_time"] = value
        state["waiting_for"] = None
        await set_setting("start_time", value)
        await add_log(f"🕐 Orario partenza impostato: {value}")

        await update.message.reply_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )
        return

    # =====================================================
    # ID MANUALI
    # =====================================================

    if target in ("manual_ids", "contact_ids"):

        raw = update.message.text
        members = []
        seen = set()

        # Formato: 1. @username — 123456789
        page_pattern = re.compile(
            r"^\s*\d+\.\s+(.+?)\s+[—–-]\s*(\d+)\s*$"
        )
        username_pattern = re.compile(r"^@?([A-Za-z0-9_]{5,32})$")

        for line in raw.splitlines():
            line = line.strip()
            if not line:
                continue

            match = page_pattern.match(line)
            if match:
                label = match.group(1).strip()
                user_id = int(match.group(2))
                username = None
                username_match = re.search(r"@([A-Za-z0-9_]{5,32})", label)
                if username_match:
                    username = username_match.group(1)

                key = (user_id, username)
                if key not in seen:
                    seen.add(key)
                    members.append({
                        "id": user_id,
                        "username": username,
                        "label": label,
                    })
                continue

            # Singolo ID numerico.
            if line.isdigit():
                user_id = int(line)
                key = (user_id, None)
                if key not in seen:
                    seen.add(key)
                    members.append({
                        "id": user_id,
                        "username": None,
                        "label": None,
                    })
                continue

            # Username con o senza @, uno per riga.
            username_match = username_pattern.match(line)
            if username_match:
                username = username_match.group(1)
                key = (None, username.lower())
                if key not in seen:
                    seen.add(key)
                    members.append({
                        "id": None,
                        "username": username,
                        "label": "@" + username,
                    })

        if not members:
            await update.message.reply_text(
                "❌ Non ho trovato utenti o ID Telegram validi."
            )
            return

        members = members[:20]
        is_contact_batch = (target == "contact_ids")
        state["manual_refs"] = members
        state["manual_ids"] = [
            item["id"] for item in members if item["id"] is not None
        ]
        state["waiting_for"] = None

        preview_lines = []
        for index, item in enumerate(members, start=1):
            user_id = item["id"]
            username = item["username"]
            label = item["label"]

            if label and user_id is not None:
                preview_lines.append(f"{index}. {label} — {user_id}")
            elif username:
                preview_lines.append(f"{index}. @{username}")
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
            ("📒 CONFERMA CONTATTI RUBRICA\n\n" if is_contact_batch else "👤 CONFERMA INVITO MANUALE\n\n")
            + f"📤 Destinazione:\n{state['group_b']['name']}\n\n"
            f"Utenti riconosciuti: {len(members)}\n\n"
            + "\n".join(preview_lines)
            + lock_note
            + "\n\nProcedere?",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "✅ CONFERMA",
                    callback_data=("contacts_confirm" if is_contact_batch else "manual_confirm"),
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
        "V4.4.3"
    )

    global scheduler_task
    scheduler_task = asyncio.create_task(
        daily_scheduler(application)
    )


async def post_shutdown(
    application,
):

    global scheduler_task
    if scheduler_task:
        scheduler_task.cancel()

    state["running"] = False
    state["stop_requested"] = True

    await user_client.disconnect()


# =========================================================
# BENVENUTO NUOVI MEMBRI GRUPPO B
# =========================================================

WELCOME_DELETE_SECONDS = 10 * 60
CHANNEL_URL = "https://t.me/bestprice_2026"


def _same_telegram_chat_id(a, b):
    """Confronta in modo tollerante gli ID chat Telethon/Bot API."""
    try:
        a = int(a)
        b = int(b)
    except (TypeError, ValueError):
        return False

    if a == b:
        return True

    # Un Channel Telethon può essere salvato come ID positivo, mentre
    # il Bot API lo espone nel formato -100xxxxxxxxxx.
    a_abs = str(abs(a))
    b_abs = str(abs(b))
    return a_abs.removeprefix("100") == b_abs.removeprefix("100")


async def _delete_welcome_later(bot, chat_id, message_id):
    await asyncio.sleep(WELCOME_DELETE_SECONDS)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning("Impossibile eliminare il benvenuto %s: %s", message_id, e)


async def delete_left_member_notice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Elimina il messaggio di servizio quando qualcuno lascia il Gruppo B."""
    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat or not message.left_chat_member:
        return

    # Agisce esclusivamente nel Gruppo B configurato.
    if not state.get("group_b") or not _same_telegram_chat_id(
        chat.id,
        state["group_b"].get("id"),
    ):
        return

    try:
        await context.bot.delete_message(
            chat_id=chat.id,
            message_id=message.message_id,
        )
    except Exception as e:
        logger.warning(
            "Impossibile eliminare il messaggio di uscita %s: %s",
            message.message_id,
            e,
        )


async def welcome_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Invia un solo benvenuto personale ai nuovi membri del Gruppo B."""
    message = update.effective_message
    chat = update.effective_chat

    if not message or not chat or not message.new_chat_members:
        return

    # La funzione deve lavorare esclusivamente nel Gruppo B configurato.
    if not state.get("group_b") or not _same_telegram_chat_id(
        chat.id,
        state["group_b"].get("id"),
    ):
        return

    # Elimina subito il messaggio di servizio Telegram
    # (es. "Mario si è unito al gruppo").
    try:
        await context.bot.delete_message(
            chat_id=chat.id,
            message_id=message.message_id,
        )
    except Exception as e:
        logger.warning(
            "Impossibile eliminare il messaggio di ingresso %s: %s",
            message.message_id,
            e,
        )

    for member in message.new_chat_members:
        if member.is_bot:
            continue

        if member.username:
            person = f"@{html.escape(member.username)}"
        else:
            visible_name = html.escape(member.full_name or "nuovo membro")
            person = f'<a href="tg://user?id={member.id}">{visible_name}</a>'

        text = (
            f"👋 <b>Benvenuto {person} nella Community BestPrice24h!</b>\n\n"
            "🔥 Sei nel posto giusto per scoprire <b>offerte, ribassi di prezzo e occasioni Amazon</b> "
            "selezionate ogni giorno.\n\n"
            "📲 Le offerte vengono pubblicate sul nostro <b>canale ufficiale BestPrice24h</b>, "
            "così puoi trovarle subito senza perderti tra centinaia di prodotti.\n\n"
            "💡 <b>Il consiglio:</b> entra nel canale e attiva le notifiche per non perdere "
            "le occasioni migliori.\n\n"
            "👇 <b>Ci vediamo nel canale!</b>"
        )

        sent = await context.bot.send_message(
            chat_id=chat.id,
            text=text,
            parse_mode="HTML",
            disable_web_page_preview=True,
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("🔥 SCOPRI LE OFFERTE", url=CHANNEL_URL)]
            ]),
        )

        asyncio.create_task(
            _delete_welcome_later(context.bot, chat.id, sent.message_id)
        )



# =========================================================
# MODERAZIONE GRUPPO B
# =========================================================

_LINK_RE = re.compile(
    r"(?i)(?:https?://|www\.|t\.me/|telegram\.me/|(?:[a-z0-9-]+\.)+(?:com|it|net|org|eu|io|co|me|app|dev|info|biz)(?:/|\b))"
)


def _message_contains_link(message):
    """Rileva link espliciti, link Telegram e URL incorporati nel testo/caption."""
    if not message:
        return False

    for entity in list(message.entities or []) + list(message.caption_entities or []):
        if entity.type in ("url", "text_link"):
            return True

    content = (message.text or message.caption or "").strip()
    return bool(content and _LINK_RE.search(content))


async def moderate_group_b_links(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Nel Gruppo B elimina silenziosamente i messaggi degli utenti che contengono link."""
    message = update.effective_message
    chat = update.effective_chat
    user = update.effective_user

    if not message or not chat:
        return

    # Moderazione esclusivamente nel Gruppo B configurato.
    if not state.get("group_b") or not _same_telegram_chat_id(
        chat.id,
        state["group_b"].get("id"),
    ):
        return

    # Non toccare i messaggi inviati dal bot stesso.
    if user and user.is_bot:
        return

    if not _message_contains_link(message):
        return

    try:
        await context.bot.delete_message(
            chat_id=chat.id,
            message_id=message.message_id,
        )
        who = (
            f"@{user.username}"
            if user and user.username
            else (user.full_name if user else "utente")
        )
        await add_log(f"🗑 Link eliminato nel Gruppo B — {who}")
    except Exception as e:
        logger.warning(
            "Impossibile eliminare messaggio con link nel Gruppo B %s: %s",
            message.message_id,
            e,
        )
        await add_log("⚠️ Link rilevato nel Gruppo B ma eliminazione non riuscita")

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
            filters.StatusUpdate.NEW_CHAT_MEMBERS,
            welcome_new_members,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.StatusUpdate.LEFT_CHAT_MEMBER,
            delete_left_member_notice,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.ChatType.GROUPS,
            moderate_group_b_links,
        )
    )

    application.add_handler(
        MessageHandler(
            filters.ChatType.PRIVATE
            & filters.TEXT
            & ~filters.COMMAND,
            text_input,
        )
    )

    logger.info(
        "BestPrice Member Manager "
        "V4.5.2 avviato"
    )

    application.run_polling()


if __name__ == "__main__":
    main()
