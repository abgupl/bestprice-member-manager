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

# Solo questo account Telegram può utilizzare il bot
ADMIN_USER_ID = int(os.environ["ADMIN_USER_ID"])


logging.basicConfig(
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    level=logging.INFO,
)

logger = logging.getLogger(__name__)


# =========================================================
# TELETHON / ACCOUNT OPERATIVO
# =========================================================

user_client = TelegramClient(
    StringSession(TELEGRAM_SESSION),
    API_ID,
    API_HASH,
)


# =========================================================
# SICUREZZA
# =========================================================

def is_admin(update: Update) -> bool:
    """
    Restituisce True esclusivamente se il comando arriva
    dall'account Telegram autorizzato.
    """
    user = update.effective_user

    return (
        user is not None
        and user.id == ADMIN_USER_ID
    )


async def deny_access(update: Update):
    """
    Risposta generica agli account non autorizzati.
    Non mostra menu, gruppi o informazioni interne.
    """

    logger.warning(
        "Tentativo accesso non autorizzato. User ID: %s",
        update.effective_user.id
        if update.effective_user
        else "sconosciuto"
    )

    if update.callback_query:

        await update.callback_query.answer(
            "⛔ Accesso non autorizzato.",
            show_alert=True
        )

    elif update.effective_message:

        await update.effective_message.reply_text(
            "⛔ Accesso non autorizzato."
        )


# =========================================================
# LETTURA GRUPPI
# =========================================================

async def get_groups():
    """
    Versione TEST.

    Legge i gruppi accessibili all'account operativo.

    Questa funzione NON:
    - invita utenti
    - rimuove utenti
    - invia messaggi
    - modifica gruppi
    """

    groups = []

    async for dialog in user_client.iter_dialogs():

        entity = dialog.entity

        # Supergruppi
        if isinstance(entity, Channel) and entity.megagroup:

            groups.append(
                {
                    "id": entity.id,
                    "name": dialog.name,
                    "type": "Supergruppo",
                }
            )

        # Gruppi Telegram classici
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
# MENU
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


# =========================================================
# /START
# =========================================================

async def start(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    # SICUREZZA
    if not is_admin(update):

        await deny_access(update)

        return

    text = (
        "👥 BESTPRICE MEMBER MANAGER\n\n"
        "🔐 Accesso amministratore autorizzato\n\n"
        "📥 Gruppo A: non impostato\n"
        "📤 Gruppo B: non impostato\n"
        "🎯 Limite futuro: 5/giorno\n"
        "🧪 Modalità: TEST\n\n"
        "Nessun utente verrà invitato "
        "in questa modalità."
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
            f"👥 Gruppi accessibili: {len(groups)}\n\n"
            "🔐 Pannello protetto tramite ADMIN_USER_ID\n\n"
            "MTProto/Telethon funziona correttamente."
        )

    except Exception as e:

        logger.exception(
            "Errore test connessione"
        )

        text = (
            "❌ ERRORE CONNESSIONE\n\n"
            f"{type(e).__name__}\n\n"
            "Controlla API_ID, API_HASH "
            "e TELEGRAM_SESSION."
        )

    await query.edit_message_text(
        text,
        reply_markup=main_keyboard()
    )


# =========================================================
# MOSTRA GRUPPI
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

        # Massimo 30 gruppi nel test
        for group in groups[:30]:

            callback = (
                f"select_{target}_{group['id']}"
            )

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

        if target == "a":

            title = "📥 SELEZIONA GRUPPO A"

        else:

            title = "📤 SELEZIONA GRUPPO B"

        await query.edit_message_text(
            f"{title}\n\n"
            "Seleziona il gruppo:",
            reply_markup=InlineKeyboardMarkup(
                keyboard
            )
        )

    except Exception as e:

        logger.exception(
            "Errore caricamento gruppi"
        )

        await query.edit_message_text(
            "❌ Errore nel caricamento gruppi:\n\n"
            f"{type(e).__name__}",
            reply_markup=main_keyboard()
        )


# =========================================================
# CALLBACK PULSANTI
# =========================================================

async def buttons(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    # =====================================================
    # CONTROLLO SICUREZZA PRIMA DI QUALSIASI OPERAZIONE
    # =====================================================

    if not is_admin(update):

        await deny_access(update)

        return

    query = update.callback_query

    await query.answer()

    data = query.data


    # HOME
    if data == "home":

        await query.edit_message_text(
            "👥 BESTPRICE MEMBER MANAGER\n\n"
            "🔐 Accesso amministratore\n"
            "🧪 Modalità TEST\n\n"
            "Nessun utente viene invitato.",
            reply_markup=main_keyboard()
        )


    # TEST
    elif data == "test":

        await test_connection(query)


    # GRUPPO A
    elif data == "group_a":

        await show_groups(
            query,
            "a"
        )


    # GRUPPO B
    elif data == "group_b":

        await show_groups(
            query,
            "b"
        )


    # STATO
    elif data == "status":

        group_a = context.bot_data.get(
            "group_a",
            "non impostato"
        )

        group_b = context.bot_data.get(
            "group_b",
            "non impostato"
        )

        await query.edit_message_text(
            "📊 STATO\n\n"
            "🔐 Amministratore: AUTORIZZATO\n\n"
            f"📥 Gruppo A: {group_a}\n"
            f"📤 Gruppo B: {group_b}\n\n"
            "🧪 Modalità: TEST\n"
            "⏸ Inviti: DISATTIVATI\n"
            "🎯 Limite futuro: 5/giorno",
            reply_markup=main_keyboard()
        )


    # SELEZIONE GRUPPO A
    elif data.startswith(
        "select_a_"
    ):

        group_id = data.replace(
            "select_a_",
            ""
        )

        context.bot_data[
            "group_a"
        ] = group_id

        await query.edit_message_text(
            "✅ GRUPPO A SELEZIONATO\n\n"
            f"ID: {group_id}",
            reply_markup=main_keyboard()
        )


    # SELEZIONE GRUPPO B
    elif data.startswith(
        "select_b_"
    ):

        group_id = data.replace(
            "select_b_",
            ""
        )

        context.bot_data[
            "group_b"
        ] = group_id

        await query.edit_message_text(
            "✅ GRUPPO B SELEZIONATO\n\n"
            f"ID: {group_id}",
            reply_markup=main_keyboard()
        )


# =========================================================
# AVVIO TELETHON
# =========================================================

async def post_init(application):

    logger.info(
        "Connessione account MTProto..."
    )

    await user_client.connect()

    if not await user_client.is_user_authorized():

        raise RuntimeError(
            "TELEGRAM_SESSION non autorizzata."
        )

    me = await user_client.get_me()

    logger.info(
        "Account MTProto collegato: %s | ID: %s",
        me.first_name,
        me.id
    )

    logger.info(
        "ADMIN_USER_ID configurato: %s",
        ADMIN_USER_ID
    )


# =========================================================
# CHIUSURA
# =========================================================

async def post_shutdown(application):

    logger.info(
        "Disconnessione Telethon..."
    )

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
            start
        )
    )

    application.add_handler(
        CallbackQueryHandler(
            buttons
        )
    )

    logger.info(
        "Avvio BestPrice Member Manager..."
    )

    application.run_polling()


if __name__ == "__main__":

    main()
