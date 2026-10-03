import os
import asyncio
import logging
from datetime import datetime, date

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
# STATO RUNTIME
# =========================================================

state = {
    "group_a": None,
    "group_b": None,

    # Primo test prudente
    "daily_target": 1,
    "max_attempts": 3,

    "running": False,
    "stop_requested": False,
    "waiting_for": None,

    "migrated_today": 0,
    "attempts_today": 0,
    "privacy_today": 0,
    "already_today": 0,
    "errors_today": 0,

    "last_event": "Nessuna attività",
}


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

        await db.commit()


async def add_log(message, level="INFO"):

    now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    state["last_event"] = message

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO logs(created_at, level, message)
            VALUES (?, ?, ?)
            """,
            (now, level, message),
        )

        await db.commit()

    logger.info("%s | %s", level, message)


async def save_processed(user, status):

    source_id = state["group_a"]["id"]
    destination_id = state["group_b"]["id"]

    username = getattr(user, "username", None)

    display_name = " ".join(
        x for x in [
            getattr(user, "first_name", None),
            getattr(user, "last_name", None),
        ]
        if x
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
                datetime.now().isoformat(),
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

def is_admin(update: Update):

    user = update.effective_user

    return (
        user is not None
        and user.id == ADMIN_USER_ID
    )


async def deny_access(update: Update):

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

        value = value.split("t.me/", 1)[1]
        value = value.split("?", 1)[0]
        value = value.strip("/")

        if value.startswith("+"):
            raise ValueError(
                "Per gruppi privati usa l'ID Telegram."
            )

        value = "@" + value.lstrip("@")

    if value.lstrip("-").isdigit():
        value = int(value)

    entity = await user_client.get_entity(value)

    if not isinstance(entity, (Channel, Chat)):
        raise ValueError("Non è un gruppo Telegram.")

    # Un Channel deve essere un megagroup, non un canale broadcast
    if isinstance(entity, Channel) and not entity.megagroup:
        raise ValueError(
            "È un canale, non un gruppo/supergruppo."
        )

    return {
        "id": entity.id,
        "name": getattr(entity, "title", "Gruppo"),
        "input": value,
    }


# =========================================================
# MENU
# =========================================================

def group_label(group):

    if not group:
        return "Non impostato"

    name = group["name"]

    if len(name) > 27:
        name = name[:24] + "..."

    return name


def main_keyboard():

    return InlineKeyboardMarkup([
        [
            InlineKeyboardButton(
                "📥 IMPOSTA A",
                callback_data="set_a",
            ),
            InlineKeyboardButton(
                "📤 IMPOSTA B",
                callback_data="set_b",
            ),
        ],
        [
            InlineKeyboardButton(
                "➖",
                callback_data="target_minus",
            ),
            InlineKeyboardButton(
                f"🎯 {state['daily_target']}",
                callback_data="noop",
            ),
            InlineKeyboardButton(
                "➕",
                callback_data="target_plus",
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
                "📊 STATO",
                callback_data="status",
            ),
            InlineKeyboardButton(
                "📋 LOG",
                callback_data="logs",
            ),
        ],
    ])


def home_text():

    status = (
        "🟢 ATTIVO"
        if state["running"]
        else "🔴 FERMO"
    )

    return (
        "👥 BESTPRICE MEMBER MANAGER\n\n"

        f"📥 A: {group_label(state['group_a'])}\n"
        f"📤 B: {group_label(state['group_b'])}\n\n"

        f"🎯 Obiettivo: {state['daily_target']}\n"
        f"🔎 Max tentativi/ciclo: {state['max_attempts']}\n\n"

        f"✅ Migrati oggi: {state['migrated_today']}\n"
        f"🔎 Tentativi oggi: {state['attempts_today']}\n"
        f"🛡 Privacy: {state['privacy_today']}\n"
        f"↪️ Già presenti: {state['already_today']}\n"
        f"⚠️ Errori: {state['errors_today']}\n\n"

        f"{status}\n\n"

        f"Ultimo evento:\n{state['last_event']}"
    )


# =========================================================
# RESET GIORNALIERO
# =========================================================

runtime_day = date.today()


def reset_daily_if_needed():

    global runtime_day

    if runtime_day != date.today():

        runtime_day = date.today()

        state["migrated_today"] = 0
        state["attempts_today"] = 0
        state["privacy_today"] = 0
        state["already_today"] = 0
        state["errors_today"] = 0


# =========================================================
# MOTORE MIGRAZIONE
# =========================================================

async def migration_worker(application):

    state["running"] = True
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

        async for user in user_client.iter_participants(source):

            reset_daily_if_needed()

            if not state["running"]:
                await add_log("⏸ Ciclo messo in pausa")
                break

            if state["stop_requested"]:
                await add_log("🛑 STOP manuale")
                break

            if (
                state["migrated_today"]
                >= state["daily_target"]
            ):
                await add_log(
                    "🎯 Obiettivo raggiunto"
                )
                break

            if attempts_cycle >= state["max_attempts"]:
                await add_log(
                    "🛑 Massimo tentativi del ciclo raggiunto"
                )
                break

            # -------------------------------
            # FILTRI SENZA INVITO
            # -------------------------------

            if user.id == me.id:
                continue

            if getattr(user, "bot", False):
                continue

            if getattr(user, "deleted", False):
                continue

            if await was_processed(user.id):
                continue

            display = (
                f"@{user.username}"
                if getattr(user, "username", None)
                else f"ID {user.id}"
            )

            await add_log(
                f"🔎 Candidato: {display}"
            )

            # -------------------------------
            # TENTATIVO
            # -------------------------------

            attempts_cycle += 1
            state["attempts_today"] += 1

            try:

                await user_client(
                    InviteToChannelRequest(
                        destination,
                        [user],
                    )
                )

                state["migrated_today"] += 1

                await save_processed(
                    user,
                    "MIGRATED",
                )

                await add_log(
                    f"✅ Migrato: {display}"
                )

            except UserAlreadyParticipantError:

                state["already_today"] += 1

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

                state["privacy_today"] += 1

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
                    f"⏳ FloodWait: Telegram richiede "
                    f"{e.seconds}s. STOP.",
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

                state["errors_today"] += 1

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

        # Notifica privata all'amministratore
        try:

            await application.bot.send_message(
                chat_id=ADMIN_USER_ID,
                text=(
                    "🏁 CICLO TERMINATO\n\n"
                    f"✅ Migrati oggi: "
                    f"{state['migrated_today']}\n"
                    f"🔎 Tentativi: "
                    f"{state['attempts_today']}\n"
                    f"🛡 Privacy: "
                    f"{state['privacy_today']}\n"
                    f"↪️ Già presenti: "
                    f"{state['already_today']}\n"
                    f"⚠️ Errori: "
                    f"{state['errors_today']}"
                ),
            )

        except Exception:
            logger.exception(
                "Impossibile inviare notifica finale"
            )

    except Exception as e:

        state["running"] = False

        await add_log(
            f"❌ Errore motore: {type(e).__name__}",
            "ERROR",
        )

        logger.exception(
            "Errore migration_worker"
        )


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

    reset_daily_if_needed()

    await update.message.reply_text(
        home_text(),
        reply_markup=main_keyboard(),
    )


# =========================================================
# CALLBACK
# =========================================================

async def buttons(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    if not is_admin(update):
        await deny_access(update)
        return

    query = update.callback_query
    await query.answer()

    reset_daily_if_needed()

    data = query.data

    if data == "set_a":

        state["waiting_for"] = "a"

        await query.edit_message_text(
            "📥 IMPOSTA GRUPPO A\n\n"
            "Inserisci @username, link t.me "
            "oppure ID del gruppo sorgente.\n\n"
            "A deve essere il gruppo costituito "
            "da membri che hanno accettato la migrazione.",
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "❌ ANNULLA",
                    callback_data="home",
                )
            ]]),
        )

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

    elif data == "target_minus":

        if state["daily_target"] > 1:
            state["daily_target"] -= 1

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "target_plus":

        if state["daily_target"] < 5:
            state["daily_target"] += 1

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "start_run":

        if state["running"]:

            await query.answer(
                "Il ciclo è già attivo.",
                show_alert=True,
            )
            return

        if not state["group_a"] or not state["group_b"]:

            await query.answer(
                "Imposta prima A e B.",
                show_alert=True,
            )
            return

        if state["group_a"]["id"] == state["group_b"]["id"]:

            await query.answer(
                "A e B non possono essere lo stesso gruppo.",
                show_alert=True,
            )
            return

        await query.edit_message_text(
            "⚠️ CONFERMA MIGRAZIONE\n\n"
            f"📥 Da: {state['group_a']['name']}\n"
            f"📤 A: {state['group_b']['name']}\n\n"
            f"🎯 Obiettivo: {state['daily_target']}\n"
            f"🔎 Max tentativi: {state['max_attempts']}\n\n"
            "Avviare il ciclo?",
            reply_markup=InlineKeyboardMarkup([
                [
                    InlineKeyboardButton(
                        "✅ CONFERMA",
                        callback_data="confirm_run",
                    ),
                    InlineKeyboardButton(
                        "❌ ANNULLA",
                        callback_data="home",
                    ),
                ]
            ]),
        )

    elif data == "confirm_run":

        state["running"] = True

        await query.edit_message_text(
            "🟢 CICLO AVVIATO\n\n"
            "Puoi controllare l'attività dal LOG.",
            reply_markup=main_keyboard(),
        )

        asyncio.create_task(
            migration_worker(context.application)
        )

    elif data == "pause":

        state["running"] = False

        await add_log(
            "⏸ Pausa richiesta dall'amministratore"
        )

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "stop":

        state["stop_requested"] = True
        state["running"] = False

        await add_log(
            "🛑 STOP richiesto dall'amministratore"
        )

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "status":

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "logs":

        async with aiosqlite.connect(DB_PATH) as db:

            cursor = await db.execute(
                """
                SELECT created_at, message
                FROM logs
                ORDER BY id DESC
                LIMIT 15
                """
            )

            rows = await cursor.fetchall()

        if rows:

            lines = []

            for created_at, message in reversed(rows):

                try:
                    time_part = created_at.split(" ")[1][:8]
                except Exception:
                    time_part = created_at

                lines.append(
                    f"{time_part}  {message}"
                )

            text = (
                "📋 ULTIMI EVENTI\n\n"
                + "\n".join(lines)
            )

        else:

            text = "📋 LOG\n\nNessun evento registrato."

        await query.edit_message_text(
            text,
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "⬅️ INDIETRO",
                    callback_data="home",
                )
            ]]),
        )

    elif data == "home":

        state["waiting_for"] = None

        await query.edit_message_text(
            home_text(),
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
                "❌ A e B devono essere gruppi diversi."
            )
            return

        if target == "a":
            state["group_a"] = group
        else:
            state["group_b"] = group

        state["waiting_for"] = None

        label = "A" if target == "a" else "B"

        await add_log(
            f"⚙️ Gruppo {label} impostato: "
            f"{group['name']}"
        )

        await update.message.reply_text(
            f"✅ GRUPPO {label} IMPOSTATO\n\n"
            f"👥 {group['name']}\n"
            f"🆔 {group['id']}",
            reply_markup=main_keyboard(),
        )

    except Exception as e:

        logger.exception(
            "Errore configurazione gruppo"
        )

        await update.message.reply_text(
            "❌ Gruppo non utilizzabile.\n\n"
            f"Errore: {type(e).__name__}"
        )


# =========================================================
# INIT / SHUTDOWN
# =========================================================

async def post_init(application):

    await init_db()

    await user_client.connect()

    if not await user_client.is_user_authorized():

        raise RuntimeError(
            "TELEGRAM_SESSION non autorizzata"
        )

    me = await user_client.get_me()

    logger.info(
        "Account operativo: %s (%s)",
        me.first_name,
        me.id,
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
        CommandHandler("start", start)
    )

    application.add_handler(
        CallbackQueryHandler(buttons)
    )

    application.add_handler(
        MessageHandler(
            filters.TEXT & ~filters.COMMAND,
            text_input,
        )
    )

    logger.info(
        "BestPrice Member Manager V4 avviato"
    )

    application.run_polling()


if __name__ == "__main__":
    main()