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

    "last_event": "Nessuna attività",
}

worker_task = None
worker_lock = asyncio.Lock()


# =========================================================
# DATA / ORA ITALIANA
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

        # Compatibilità con DB V4.1 già esistente
        try:
            await db.execute(
                "ALTER TABLE daily_stats "
                "ADD COLUMN unconfirmed INTEGER DEFAULT 0"
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
            "SELECT value FROM settings WHERE key = ?",
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
        await get_setting("daily_target", "1")
    )

    state["max_attempts"] = int(
        await get_setting("max_attempts", "3")
    )

    state["interval_minutes"] = int(
        await get_setting("interval_minutes", "10")
    )

    state["telegram_locked"] = (
        await get_setting("telegram_locked", "0") == "1"
    )

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

            if str(input_value).lstrip("-").isdigit():
                input_value = int(input_value)

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
            INSERT OR IGNORE INTO daily_stats(day)
            VALUES (?)
            """,
            (today_it(),),
        )

        await db.commit()


async def increment_stat(field, amount=1):

    allowed = {
        "migrated",
        "attempts",
        "privacy",
        "already",
        "errors",
        "unconfirmed",
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

async def add_log(message, level="INFO"):

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

    logger.info("%s | %s", level, message)


async def clear_logs():

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute("DELETE FROM logs")
        await db.commit()

    state["last_event"] = "Log cancellato"


# =========================================================
# PROCESSATI
# =========================================================

async def save_processed(user, status):

    username = getattr(user, "username", None)

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
# VERIFICA MEMBERSHIP B
# =========================================================

async def verify_in_destination(
    destination,
    user,
):

    """
    Verifica se Telegram considera realmente
    l'utente partecipante del gruppo B.

    Restituisce True/False.
    """

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

async def interruptible_wait(minutes):

    total_seconds = minutes * 60

    for _ in range(total_seconds):

        if (
            not state["running"]
            or state["stop_requested"]
            or state["telegram_locked"]
        ):
            return False

        await asyncio.sleep(1)

    return True


# =========================================================
# INTERFACCIA
# =========================================================

def group_label(group):

    if not group:
        return "Non impostato"

    name = group["name"]

    if len(name) > 24:
        return name[:21] + "..."

    return name


def main_keyboard():

    keyboard = [
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
                callback_data="interval_minus",
            ),
            InlineKeyboardButton(
                f"⏱ {state['interval_minutes']} MIN",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="interval_plus",
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

    stats = await get_today_stats()

    remaining = max(
        0,
        state["daily_target"]
        - stats["migrated"],
    )

    if state["telegram_locked"]:
        status = "🔒 INVITI SOSPESI"

    elif state["running"]:
        status = "🟢 ATTIVO"

    else:
        status = "🔴 FERMO"

    return (
        "👥 BESTPRICE MEMBER MANAGER V4.2\n\n"

        f"📥 A: {group_label(state['group_a'])}\n"
        f"📤 B: {group_label(state['group_b'])}\n\n"

        "📅 OGGI\n"
        f"✅ Confermati: "
        f"{stats['migrated']}/"
        f"{state['daily_target']}\n"

        f"🎯 Rimanenti: {remaining}\n"

        f"⏱ Intervallo: "
        f"{state['interval_minutes']} min\n"

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

        f"🕐 {now_it().strftime('%H:%M:%S')}\n"
        f"Ultimo evento:\n"
        f"{state['last_event']}"
    )


# =========================================================
# MIGRATION WORKER
# =========================================================

async def migration_worker(application):

    global worker_task

    async with worker_lock:

        if not state["running"]:
            return

        if state["telegram_locked"]:
            state["running"] = False
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
                    break

                if state["stop_requested"]:
                    break

                if state["telegram_locked"]:
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
                        "🛑 Max tentativi raggiunto"
                    )

                    break

                # -------------------------
                # FILTRI
                # -------------------------

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

                attempts_cycle += 1

                await increment_stat(
                    "attempts"
                )

                await add_log(
                    f"🔎 Candidato: {display}"
                )

                try:

                    # =====================
                    # INVITO
                    # =====================

                    await user_client(
                        InviteToChannelRequest(
                            destination,
                            [user],
                        )
                    )

                    # Lasciamo a Telegram il tempo
                    # di aggiornare lo stato
                    await asyncio.sleep(3)

                    confirmed = (
                        await verify_in_destination(
                            destination,
                            user,
                        )
                    )

                    # =====================
                    # CONFERMATO
                    # =====================

                    if confirmed:

                        await increment_stat(
                            "migrated"
                        )

                        await save_processed(
                            user,
                            "CONFIRMED",
                        )

                        await add_log(
                            f"✅ Confermato in B: "
                            f"{display}"
                        )

                    # =====================
                    # NON CONFERMATO
                    # =====================

                    else:

                        await increment_stat(
                            "unconfirmed"
                        )

                        await save_processed(
                            user,
                            "UNCONFIRMED",
                        )

                        await add_log(
                            f"⚠️ Invito non confermato: "
                            f"{display}",
                            "WARNING",
                        )

                # =========================
                # GIÀ PRESENTE
                # =========================

                except UserAlreadyParticipantError:

                    await increment_stat(
                        "already"
                    )

                    await save_processed(
                        user,
                        "ALREADY",
                    )

                    await add_log(
                        f"↪️ Già presente: "
                        f"{display}"
                    )

                # =========================
                # PRIVACY
                # =========================

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
                        f"🛡 Privacy/non invitabile: "
                        f"{display}"
                    )

                # =========================
                # FLOOD WAIT
                # =========================

                except FloodWaitError as e:

                    state["running"] = False

                    await add_log(
                        f"⏳ FloodWait "
                        f"{e.seconds}s. STOP.",
                        "WARNING",
                    )

                    break

                # =========================
                # PEER FLOOD
                # =========================

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

                    break

                # =========================
                # ALTRO ERRORE
                # =========================

                except Exception as e:

                    await increment_stat(
                        "errors"
                    )

                    await add_log(
                        f"❌ {display}: "
                        f"{type(e).__name__}",
                        "ERROR",
                    )

                # =========================
                # CONTROLLO TARGET
                # =========================

                stats = await get_today_stats()

                if (
                    stats["migrated"]
                    >= state["daily_target"]
                ):

                    await add_log(
                        "🎯 Obiettivo giornaliero raggiunto"
                    )

                    break

                # =========================
                # TIMER
                # =========================

                if (
                    state["running"]
                    and not state["telegram_locked"]
                ):

                    await add_log(
                        f"⏱ Prossima operazione tra "
                        f"{state['interval_minutes']} minuti"
                    )

                    completed = await interruptible_wait(
                        state["interval_minutes"]
                    )

                    if not completed:
                        break

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

                        f"✅ Confermati: "
                        f"{stats['migrated']}/"
                        f"{state['daily_target']}\n"

                        f"🔎 Tentativi: "
                        f"{stats['attempts']}\n"

                        f"🛡 Privacy: "
                        f"{stats['privacy']}\n"

                        f"↪️ Già presenti: "
                        f"{stats['already']}\n"

                        f"⚠️ Non confermati: "
                        f"{stats['unconfirmed']}\n"

                        f"❌ Errori: "
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

    # =====================================================
    # GRUPPO A
    # =====================================================

    if data == "set_a":

        state["waiting_for"] = "a"

        await query.edit_message_text(
            "📥 IMPOSTA GRUPPO A\n\n"
            "Inserisci @username, link t.me "
            "oppure ID.",
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
            "Inserisci @username, link t.me "
            "oppure ID.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # TARGET 1-50
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
    # INTERVALLO 10-120 MIN
    # =====================================================

    elif data == "interval_minus":

        if state["interval_minutes"] > 10:

            state["interval_minutes"] -= 5

            await set_setting(
                "interval_minutes",
                state["interval_minutes"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "interval_plus":

        if state["interval_minutes"] < 120:

            state["interval_minutes"] += 5

            await set_setting(
                "interval_minutes",
                state["interval_minutes"],
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # MAX TENTATIVI
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
    # AVVIA
    # =====================================================

    elif data == "start_run":

        if state["telegram_locked"]:

            await query.answer(
                "🔒 Inviti sospesi dopo una "
                "restrizione Telegram.",
                show_alert=True,
            )

            return

        if state["running"]:

            await query.answer(
                "Processo già attivo.",
                show_alert=True,
            )

            return

        if (
            worker_task is not None
            and not worker_task.done()
        ):

            await query.answer(
                "Esiste già un worker attivo.",
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
                "🎯 Limite giornaliero raggiunto.",
                show_alert=True,
            )

            return

        await query.edit_message_text(
            "⚠️ CONFERMA AVVIO\n\n"

            f"📥 Da: "
            f"{state['group_a']['name']}\n"

            f"📤 A: "
            f"{state['group_b']['name']}\n\n"

            f"🎯 Target: "
            f"{state['daily_target']}/giorno\n"

            f"✅ Confermati oggi: "
            f"{stats['migrated']}\n"

            f"⏱ Intervallo: "
            f"{state['interval_minutes']} min\n"

            f"🔎 Max tentativi: "
            f"{state['max_attempts']}\n\n"

            "Avviare il ciclo?",

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

    # =====================================================
    # CONFERMA
    # =====================================================

    elif data == "confirm_run":

        if state["telegram_locked"]:

            await query.answer(
                "🔒 Inviti sospesi.",
                show_alert=True,
            )

            return

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
                "Limite già raggiunto.",
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

    # =====================================================
    # PAUSA
    # =====================================================

    elif data == "pause":

        if state["running"]:

            state["running"] = False

            await add_log(
                "⏸ Pausa richiesta"
            )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # STOP
    # =====================================================

    elif data == "stop":

        state["stop_requested"] = True
        state["running"] = False

        await add_log(
            "🛑 STOP richiesto"
        )

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    # =====================================================
    # STATISTICHE
    # =====================================================

    elif data == "statistics":

        today = await get_today_stats()
        total = await get_total_stats()

        text = (
            "📊 STATISTICHE\n\n"

            "📅 OGGI\n"

            f"✅ Confermati: "
            f"{today['migrated']}/"
            f"{state['daily_target']}\n"

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
                SELECT created_at, message
                FROM logs
                ORDER BY id DESC
                LIMIT 20
                """
            )

            rows = await cursor.fetchall()

        lines = []

        for created_at, message in reversed(rows):

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

    # =====================================================
    # PULISCI LOG
    # =====================================================

    elif data == "clear_logs_confirm":

        await query.edit_message_text(
            "⚠️ Cancellare il LOG?\n\n"
            "Statistiche e utenti processati "
            "rimarranno memorizzati.",
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
                    "⬅️ INDIETRO",
                    callback_data="home",
                )
            ]]),
        )

    # =====================================================
    # SBLOCCO MANUALE
    # =====================================================

    elif data == "unlock_confirm":

        await query.edit_message_text(
            "⚠️ RIABILITARE GLI INVITI?\n\n"
            "Procedi solo dopo aver verificato "
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

        state["telegram_locked"] = False

        await set_setting(
            "telegram_locked",
            "0",
        )

        await add_log(
            "🔓 Blocco inviti rimosso manualmente"
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

        await query.edit_message_text(
            await home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "noop":
        pass


# =========================================================
# INPUT A/B
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
            "💾 Salvato.",
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
        "Timezone: Europe/Rome"
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
        "BestPrice Member Manager V4.2 avviato"
    )

    application.run_polling()


if __name__ == "__main__":
    main()