import os
import logging

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    ContextTypes,
)

from telethon import TelegramClient
from telethon.sessions import StringSession
from telethon.tl.types import Channel, Chat


# =========================================================
# CONFIGURAZIONE
# =========================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]

logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
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


async def get_groups():
    """Restituisce i gruppi accessibili all'account operativo."""

    groups = []

    async for dialog in user_client.iter_dialogs():

        entity = dialog.entity

        # Supergruppo Telegram
        if isinstance(entity, Channel) and entity.megagroup:
            groups.append(
                {
                    "id": entity.id,
                    "name": dialog.name,
                    "type": "Supergruppo",
                }
            )

        # Gruppo classico
        elif isinstance(entity, Chat):
            groups.append(
                {
                    "id": entity.id,
                    "name": dialog.name,
                    "type": "Gruppo",
                }
            )

    return groups


# =========================================================
# MENU PRINCIPALE
# =========================================================

def main_keyboard():

    keyboard = [
        [
            InlineKeyboardButton(
                "📥 GRUPPO A",
                callback_data="group_a"
            ),
            InlineKeyboardButton(
                "📤 GRUPPO B",
                callback_data="group_b"
            ),
        ],
        [
            InlineKeyboardButton(
                "🔍 TEST CONNESSIONE",
                callback_data="test"
            )
        ],
        [
            InlineKeyboardButton(
                "📊 STATO",
                callback_data="status"
            )
        ],
    ]

    return InlineKeyboardMarkup(keyboard)


async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    text = (
        "👥 BESTPRICE MEMBER MANAGER\n\n"
        "📥 Gruppo A: non impostato\n"
        "📤 Gruppo B: non impostato\n"
        "🎯 Limite futuro: 5/giorno\n"
        "🧪 Modalità: TEST\n\n"
        "Nessun utente verrà invitato in questa modalità."
    )

    await update.message.reply_text(
        text,
        reply_markup=main_keyboard()
    )


# =========================================================
# TEST CONNESSIONE
# =========================================================

async def test_connection(query):

    try:

        me = await user_client.get_me()

        groups = await get_groups()

        name = me.first_name or "Account Telegram"

        text = (
            "✅ CONNESSIONE RIUSCITA\n\n"
            f"👤 Account operativo: {name}\n"
            f"👥 Gruppi trovati: {len(groups)}\n\n"
            "MTProto/Telethon funziona correttamente."
        )

    except Exception as e:

        logger.exception("Errore test connessione")

        text = (
            "❌ ERRORE CONNESSIONE\n\n"
            f"{type(e).__name__}\n\n"
            "Controlla API_ID, API_HASH e TELEGRAM_SESSION."
        )

    await query.edit_message_text(
        text,
        reply_markup=main_keyboard()
    )


# =========================================================
# ELENCO GRUPPI
# =========================================================

async def show_groups(query, target):

    try:

        groups = await get_groups()

        if not groups:

            await query.edit_message_text(
                "⚠️ Nessun gruppo trovato.",
                reply_markup=main_keyboard()
            )

            return

        keyboard = []

        # Per il primo test mostriamo massimo 30 gruppi
        for group in groups[:30]:

            callback = f"select_{target}_{group['id']}"

            name = group["name"]

            if len(name) > 35:
                name = name[:32] + "..."

            keyboard.append(
                [
                    InlineKeyboardButton(
                        name,
                        callback_data=callback
                    )
                ]
            )

        keyboard.append(
            [
                InlineKeyboardButton(
                    "⬅️ INDIETRO",
                    callback_data="home"
                )
            ]
        )

        title = (
            "📥 SELEZIONA GRUPPO A"
            if target == "a"
            else "📤 SELEZIONA GRUPPO B"
        )

        await query.edit_message_text(
            f"{title}\n\n"
            "Seleziona uno dei gruppi accessibili "
            "all'account operativo:",
            reply_markup=InlineKeyboardMarkup(keyboard)
        )

    except Exception as e:

        logger.exception("Errore caricamento gruppi")

        await query.edit_message_text(
            f"❌ Errore nel caricamento gruppi:\n\n"
            f"{type(e).__name__}",
            reply_markup=main_keyboard()
        )


# =========================================================
# CALLBACK
# =========================================================

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):

    query = update.callback_query

    await query.answer()

    data = query.data

    if data == "home":

        await query.edit_message_text(
            "👥 BESTPRICE MEMBER MANAGER\n\n"
            "🧪 Modalità TEST\n"
            "Nessun utente viene invitato.",
            reply_markup=main_keyboard()
        )

    elif data == "test":

        await test_connection(query)

    elif data == "group_a":

        await show_groups(query, "a")

    elif data == "group_b":

        await show_groups(query, "b")

    elif data == "status":

        await query.edit_message_text(
            "📊 STATO\n\n"
            "🧪 Modalità: TEST\n"
            "⏸ Inviti: DISATTIVATI\n"
            "🎯 Limite futuro: 5/giorno",
            reply_markup=main_keyboard()
        )

    elif data.startswith("select_a_"):

        group_id = data.replace("select_a_", "")

        context.bot_data["group_a"] = group_id

        await query.edit_message_text(
            "✅ Gruppo A selezionato.\n\n"
            f"ID: {group_id}",
            reply_markup=main_keyboard()
        )

    elif data.startswith("select_b_"):

        group_id = data.replace("select_b_", "")

        context.bot_data["group_b"] = group_id

        await query.edit_message_text(
            "✅ Gruppo B selezionato.\n\n"
            f"ID: {group_id}",
            reply_markup=main_keyboard()
        )


# =========================================================
# AVVIO
# =========================================================

async def post_init(application):

    logger.info("Connessione account MTProto...")

    await user_client.connect()

    if not await user_client.is_user_authorized():

        raise RuntimeError(
            "TELEGRAM_SESSION non autorizzata."
        )

    me = await user_client.get_me()

    logger.info(
        "Account MTProto collegato: %s",
        me.first_name
    )


async def post_shutdown(application):

    await user_client.disconnect()


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

    logger.info("Avvio BestPrice Member Manager...")

    application.run_polling()


if __name__ == "__main__":
    main()
