import os
import asyncio
import logging
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
from telethon.tl.functions.channels import InviteToChannelRequest
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

    "running": False,
    "stop_requested": False,
    "waiting_for": None,

    "last_event": "Nessuna attività",
}

worker_task = None
worker_lock = asyncio.Lock()


# =========================================================
# TEMPO ITALIA
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
                errors INTEGER DEFAULT 0
            )
        """)

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
            "SELECT value FROM settings WHERE key = ?",
            (key,),
        )

        row = await cursor.fetchone()

    if row:
        return row[0]

    return default


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

    target = await get_setting(
        "daily_target",
        "1",
    )

    max_attempts = await get_setting(
        "max_attempts",
        "3",
    )

    state["daily_target"] = int(target)
    state["max_attempts"] = int(max_attempts)

    for prefix in ("group_a", "group_b"):

        group_id = await get_setting(
            f"{prefix}_id"
        )

        name = await get_setting(
            f"{prefix}_name"
        )

        input_value = await get_setting(
            f"{prefix}_input"
        )

        if group_id and name and input_value:

            # Manteniamo numerico l'ID se era numerico
            if str(input_value).lstrip("-").isdigit():
                input_value = int(input_value)

            state[prefix] = {
                "id": int(group_id),
                "name": name,
                "input": input_value,
            }


# =========================================================
# STATISTICHE GIORNALIERE
# =========================================================

async def ensure_today():

    day = today_it()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT OR IGNORE INTO daily_stats(day)
            VALUES (?)
            """,
            (day,),
        )

        await db.commit()


async def increment_stat(field, amount=1):

    allowed = {
        "migrated",
        "attempts",
        "privacy",
        "already",
        "errors",
    }

    if field not in allowed:
        raise ValueError("Campo statistico non valido")

    await ensure_today()

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            f"""
            UPDATE daily_stats
            SET {field} = {field} + ?
            WHERE day = ?
            """,
            (amount, today_it()),
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
                errors
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
    }


async def get_total_stats():

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute("""
            SELECT
                COALESCE(SUM(migrated), 0),
                COALESCE(SUM(attempts), 0),
                COALESCE(SUM(privacy), 0),
                COALESCE(SUM(already), 0),
                COALESCE(SUM(errors), 0)
            FROM daily_stats
        """)

        row = await cursor.fetchone()

    return {
        "migrated": row[0],
        "attempts": row[1],
        "privacy": row[2],
        "already": row[3],
        "errors": row[4],
    }


# =========================================================
# LOG
# =========================================================

async def add_log(message, level="INFO"):

    now = now_it().isoformat()

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
                now,
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

    state["last_event"] = "Log cancellato"


# =========================================================
# UTENTI PROCESSATI
# =========================================================

async def save_processed(user, status):

    source_id = state["group_a"]["id"]
    destination_id = state["group_b"]["id"]

    username = getattr(
        user,
        "username",
        None,
    )

    display_name = " ".join(
        value
        for value in [
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
        ]
        if value
    )

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT OR REPLACE INTO processed
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
                source_id,
                destination_id,
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
# SICUREZZA
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
                "Per gruppi privati usa l'ID Telegram."
            )

        value = "@" + value.lstrip("@")

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
# INTERFACCIA
# =========================================================

def group_label(group):

    if not group:
        return "Non impostato"

    name = group["name"]

    if len(name) > 25:
        return name[:22] + "..."

    return name


def main_keyboard():

    return InlineKeyboardMarkup([
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
        [
            InlineKeyboardButton(
                "➖",
                callback_data="target_minus",
            ),
            InlineKeyboardButton(
                f"🎯 {state['daily_target']}/GIORNO",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="target_plus",
            ),
        ],
        [
            InlineKeyboardButton(
                "➖",
                callback_data="attempts_minus",
            ),
            InlineKeyboardButton(
                f"🔎 MAX {state['max_attempts']}",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="attempts_plus",
            ),
        ],
        [
            InlineKeyboardButton(
                "▶️ AVVIA",
                callback_data="start_run",
            ),
            InlineKeyboardButton(
                "⏸ PAUSA",
                callback_data="pause",
            ),
        ],
        [
            InlineKeyboardButton(
                "🛑 STOP",
                callback_data="stop",
            ),
        ],
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
    ])


async def home_text():

    stats = await get_today_stats()

    status = (
        "🟢 ATTIVO"
        if state["running"]
        else "🔴 FERMO"
    )

    remaining = max(
        0,
        state["daily_target"]
        - stats["migrated"],
    )

    return (
        "👥 BESTPRICE MEMBER MANAGER\n\n"

        f"📥 A: {group_label(state['group_a'])}\n"
        f"📤 B: {group_label(state['group_b'])}\n\n"

        "📅 OGGI\n"
        f"✅ Migrati: "
        f"{stats['migrated']}/{state['daily_target']}\n"
        f"🎯 Rimanenti: {remaining}\n"
        f"🔎 Tentativi: {stats['attempts']}\n"
        f"🛡 Privacy: {stats['privacy']}\n"
        f"↪️ Già presenti: {stats['already']}\n"
        f"⚠️ Errori: {stats['errors']}\n\n"

        f"{status}\n\n"

        f"🕐 {now_it().strftime('%H:%M:%S')}\n"
        f"Ultimo evento: {state['last_event']}"
    )


# =========================================================
# MOTORE
# =========================================================

async def migration_worker(application):

    global worker_task

    async with worker_lock:

        if not state["running"]:
            return

        state["stop_requested"] = False

        attempts_cycle = 0

        await add_log(
            "▶️ Ciclo di migrazione avviato"
        )

        try:

            source = await user_client.get_entity(
                state["group_a"]["input"]
            )

            destination = await user_client.get_entity(
                state["group_b"]["input"]
            )

            me = await user_client.get_me()

            async for user in user_client.iter_participants(
                source
            ):

                if not state["running"]:
                    await add_log(
                        "⏸ Ciclo messo in pausa"
                    )
                    break

                if state["stop_requested"]:
                    await add_log(
                        "🛑 Ciclo interrotto"
                    )
                    break

                stats = await get_today_stats()

                if (
                    stats["migrated"]
                    >= state["daily_target"]
                ):
                    await add_log(
                        "🎯 Obiettivo giornaliero raggiunto"
                    )
                    break

                if (
                    attempts_cycle
                    >= state["max_attempts"]
                ):
                    await add_log(
                        "🛑 Max tentativi del ciclo raggiunto"
                    )
                    break

                # Filtri senza richiesta Telegram
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

                display = (
                    f"@{user.username}"
                    if getattr(
                        user,
                        "username",
                        None,
                    )
                    else f"ID {user.id}"
                )

                await add_log(
                    f"🔎 Candidato: {display}"
                )

                attempts_cycle += 1

                await increment_stat(
                    "attempts"
                )

                try:

                    await user_client(
                        InviteToChannelRequest(
                            destination,
                            [user],
                        )
                    )

                    await increment_stat(
                        "migrated"
                    )

                    await save_processed(
                        user,
                        "MIGRATED",
                    )

                    await add_log(
                        f"✅ Migrato: {display}"
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
                        f"↪️ Già presente: {display}"
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
                        f"🛡 Privacy/non invitabile: {display}"
                    )

                except FloodWaitError as e:

                    state["running"] = False

                    await add_log(
                        f"⏳ FloodWait {e.seconds}s. STOP.",
                        "WARNING",
                    )

                    break

                except PeerFloodError:

                    state["running"] = False

                    await add_log(
                        "🚫 Restrizione Telegram. STOP.",
                        "WARNING",
                    )

                    break

                except Exception as e:

                    await increment_stat(
                        "errors"
                    )

                    await save_processed(
                        user,
                        f"ERROR:{type(e).__name__}",
                    )

                    await add_log(
                        f"⚠️ {display}: "
                        f"{type(e).__name__}",
                        "ERROR",
                    )

            state["running"] = False

            await add_log(
                "🏁 Ciclo terminato"
            )

            stats = await get_today_stats()

            try:

                await application.bot.send_message(
                    chat_id=ADMIN_USER_ID,
                    text=(
                        "🏁 CICLO TERMINATO\n\n"
                        f"✅ Migrati oggi: "
                        f"{stats['migrated']}/"
                        f"{state['daily_target']}\n"
                        f"🔎 Tentativi oggi: "
                        f"{stats['attempts']}\n"
                        f"🛡 Privacy: "
                        f"{stats['privacy']}\n"
                        f"↪️ Già presenti: "
                        f"{stats['already']}\n"
                        f"⚠️ Errori: "
                        f"{stats['errors']}"
                    ),
                )

            except Exception:

                logger.exception(
                    "Errore notifica finale"
                )

        except Exception as e:

            state["running"] = False

            await add_log(
                f"❌ Errore motore: "
                f"{type(e).__name__}",
                "ERROR",
            )

            logger.exception(
                "Errore migration_worker"
            )

        finally:

            worker_task = None


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
# CALLBACK
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

    # ---------------- A ----------------

    if data == "set_a":

        state["waiting_for"] = "a"

        await query.edit_message_text(
            "📥 IMPOSTA GRUPPO A\n\n"
            "Inserisci @username, link t.me "
            "oppure ID del gruppo sorgente.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

    # ---------------- B ----------------

    elif data == "set_b":

        state["waiting_for"] = "b"

        await query.edit_message_text(
            "📤 IMPOSTA GRUPPO B\n\n"
            "Inserisci @username, link t.me "
            "oppure ID del gruppo destinazione.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

    # ---------------- TARGET ----------------

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

        if state["daily_target"] < 5:

            state["daily_target"] += 1

            await set_setting(
                "daily_target",
                state["daily_target"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # ---------------- MAX ATTEMPTS ----------------

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

        if state["max_attempts"] < 20:

            state["max_attempts"] += 1

            await set_setting(
                "max_attempts",
                state["max_attempts"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # ---------------- AVVIA ----------------

    elif data == "start_run":

        if state["running"]:

            await query.answer(
                "Il ciclo è già attivo.",
                show_alert=True,
            )
            return

        if (
            worker_task is not None
            and not worker_task.done()
        ):

            await query.answer(
                "Esiste già un processo attivo.",
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

        stats = await get_today_stats()

        if (
            stats["migrated"]
            >= state["daily_target"]
        ):

            await query.answer(
                "🎯 Limite giornaliero già raggiunto.",
                show_alert=True,
            )
            return

        await query.edit_message_text(
            "⚠️ CONFERMA AVVIO\n\n"
            f"📥 Da: {state['group_a']['name']}\n"
            f"📤 A: {state['group_b']['name']}\n\n"
            f"🎯 Limite giornaliero: "
            f"{state['daily_target']}\n"
            f"✅ Già migrati oggi: "
            f"{stats['migrated']}\n"
            f"🔎 Max tentativi/ciclo: "
            f"{state['max_attempts']}\n\n"
            "Avviare?",
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
            state["running"]
            or (
                worker_task is not None
                and not worker_task.done()
            )
        ):

            await query.answer(
                "Processo già attivo.",
                show_alert=True,
            )
            return

        stats = await get_today_stats()

        if (
            stats["migrated"]
            >= state["daily_target"]
        ):

            await query.answer(
                "Limite giornaliero raggiunto.",
                show_alert=True,
            )
            return

        state["running"] = True
        state["stop_requested"] = False

        worker_task = asyncio.create_task(
            migration_worker(
                context.application
            )
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # ---------------- PAUSA ----------------

    elif data == "pause":

        if state["running"]:

            state["running"] = False

            await add_log(
                "⏸ Pausa richiesta dall'amministratore"
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # ---------------- STOP ----------------

    elif data == "stop":

        state["stop_requested"] = True
        state["running"] = False

        await add_log(
            "🛑 STOP richiesto dall'amministratore"
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # ---------------- STATISTICHE ----------------

    elif data == "statistics":

        today = await get_today_stats()
        total = await get_total_stats()

        text = (
            "📊 STATISTICHE\n\n"

            "📅 OGGI\n"
            f"✅ Migrati: "
            f"{today['migrated']}/"
            f"{state['daily_target']}\n"
            f"🔎 Tentativi: "
            f"{today['attempts']}\n"
            f"🛡 Privacy: "
            f"{today['privacy']}\n"
            f"↪️ Già presenti: "
            f"{today['already']}\n"
            f"⚠️ Errori: "
            f"{today['errors']}\n\n"

            "📈 TOTALI\n"
            f"✅ Migrati: "
            f"{total['migrated']}\n"
            f"🔎 Tentativi: "
            f"{total['attempts']}\n"
            f"🛡 Privacy: "
            f"{total['privacy']}\n"
            f"↪️ Già presenti: "
            f"{total['already']}\n"
            f"⚠️ Errori: "
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

    # ---------------- LOG ----------------

    elif data == "logs":

        async with aiosqlite.connect(
            DB_PATH
        ) as db:

            cursor = await db.execute(
                """
                SELECT created_at, message
                FROM logs
                ORDER BY id DESC
                LIMIT 20
                """
            )

            rows = await cursor.fetchall()

        lines = []

        for created_at, message in reversed(
            rows
        ):

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

        text = (
            "📋 ULTIMI EVENTI\n\n"
            + (
                "\n".join(lines)
                if lines
                else "Nessun evento."
            )
        )

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "🔄 AGGIORNA",
                        callback_data="logs",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "🗑 PULISCI LOG",
                        callback_data="clear_logs_confirm",
                    ),
                ],
                [
                    InlineKeyboardButton(
                        "⬅️ INDIETRO",
                        callback_data="home",
                    ),
                ],
            ]),
        )

    # ---------------- CLEAR LOG ----------------

    elif data == "clear_logs_confirm":

        await query.edit_message_text(
            "⚠️ Cancellare il LOG?\n\n"
            "Gli utenti già processati e le "
            "statistiche NON verranno cancellati.",
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
            "✅ Log cancellato.\n\n"
            "Storico utenti e statistiche "
            "sono rimasti intatti.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⬅️ INDIETRO",
                    callback_data="home",
                )
            ]]),
        )

    # ---------------- HOME ----------------

    elif data == "home":

        state["waiting_for"] = None

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "noop":
        pass


# =========================================================
# INPUT GRUPPI
# =========================================================

async def text_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not is_admin(update):
        await deny_access(update)
        return

    target = state["waiting_for"]

    if target not in ("a", "b"):
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
            and other["id"] == group["id"]
        ):

            await update.message.reply_text(
                "❌ A e B devono essere diversi."
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
            f"⚙️ Gruppo {label} impostato: "
            f"{group['name']}"
        )

        await update.message.reply_text(
            f"✅ GRUPPO {label} IMPOSTATO\n\n"
            f"👥 {group['name']}\n"
            f"🆔 {group['id']}\n\n"
            "💾 Configurazione salvata.",
            reply_markup=main_keyboard(),
        )

    except Exception as e:

        logger.exception(
            "Errore impostazione gruppo"
        )

        await update.message.reply_text(
            "❌ Gruppo non utilizzabile.\n\n"
            f"Errore: {type(e).__name__}"
        )


# =========================================================
# INIT
# =========================================================

async def post_init(application):

    await init_db()
    await load_settings()
    await ensure_today()

    await user_client.connect()

    if not await user_client.is_user_authorized():

        raise RuntimeError(
            "TELEGRAM_SESSION non autorizzata."
        )

    me = await user_client.get_me()

    logger.info(
        "Account operativo: %s (%s)",
        me.first_name,
        me.id,
    )

    logger.info(
        "Timezone applicazione: Europe/Rome"
    )


async def post_shutdown(application):

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
            filters.TEXT & ~filters.COMMAND,
            text_input,
        )
    )

    logger.info(
        "BestPrice Member Manager V4.1 avviato"
    )

    application.run_polling()


if __name__ == "__main__":
    main()