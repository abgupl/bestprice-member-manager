import os
import logging

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


# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]
ADMIN_USER_ID = int(os.environ["ADMIN_USER_ID"])

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
    "daily_limit": 1,
    "running": False,
    "waiting_for": None,
    "attempts_today": 0,
    "success_today": 0,
    "privacy_today": 0,
    "errors_today": 0,
}


# =========================================================
# SICUREZZA
# =========================================================

def is_admin(update: Update) -> bool:
    user = update.effective_user
    return user is not None and user.id == ADMIN_USER_ID


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
# UTILITY
# =========================================================

def group_name(group):
    if not group:
        return "Non impostato"

    return group["name"]


def short_name(name, length=32):
    if len(name) <= length:
        return name

    return name[: length - 3] + "..."


async def resolve_group(value):
    """
    Risolve SOLO il gruppo indicato manualmente dall'amministratore.
    Non enumera i dialog/gruppi dell'account.
    """

    value = value.strip()

    # t.me/nomegruppo -> nomegruppo
    if "t.me/" in value:
        value = value.split("t.me/", 1)[1]
        value = value.split("?", 1)[0]
        value = value.strip("/")

        # I link privati +xxxx non sono risolvibili come username
        if value.startswith("+"):
            raise ValueError(
                "Per un gruppo privato usa @username oppure ID Telegram."
            )

        value = "@" + value.lstrip("@")

    # ID numerico
    if value.lstrip("-").isdigit():
        value = int(value)

    entity = await user_client.get_entity(value)

    # Evita di accettare utenti privati come A/B
    if not hasattr(entity, "title"):
        raise ValueError("Il valore indicato non è un gruppo.")

    title = getattr(entity, "title", "Gruppo")
    username = getattr(entity, "username", None)
    entity_id = entity.id

    return {
        "id": entity_id,
        "name": title,
        "username": username,
        "input": value,
    }


# =========================================================
# MENU
# =========================================================

def main_keyboard():

    run_button = (
        InlineKeyboardButton("⏸ PAUSA", callback_data="pause")
        if state["running"]
        else InlineKeyboardButton("▶️ AVVIA", callback_data="start_run")
    )

    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton(
                    "📥 IMPOSTA GRUPPO A",
                    callback_data="set_a",
                ),
                InlineKeyboardButton(
                    "📤 IMPOSTA GRUPPO B",
                    callback_data="set_b",
                ),
            ],
            [
                InlineKeyboardButton("➖", callback_data="limit_minus"),
                InlineKeyboardButton(
                    f"🎯 {state['daily_limit']}/GIORNO",
                    callback_data="noop",
                ),
                InlineKeyboardButton("➕", callback_data="limit_plus"),
            ],
            [
                run_button,
            ],
            [
                InlineKeyboardButton(
                    "📊 STATO",
                    callback_data="status",
                ),
                InlineKeyboardButton(
                    "🧪 TEST CONFIG",
                    callback_data="test_config",
                ),
            ],
        ]
    )


def home_text():

    status = "🟢 ATTIVO" if state["running"] else "🔴 FERMO"

    return (
        "👥 BESTPRICE MEMBER MANAGER\n\n"
        f"📥 Gruppo A: {short_name(group_name(state['group_a']))}\n"
        f"📤 Gruppo B: {short_name(group_name(state['group_b']))}\n\n"
        f"🎯 Limite: {state['daily_limit']} tentativi/giorno\n"
        f"📅 Oggi: {state['attempts_today']}/{state['daily_limit']}\n"
        f"✅ Riusciti: {state['success_today']}\n"
        f"🛡 Privacy/non invitabili: {state['privacy_today']}\n"
        f"⚠️ Errori: {state['errors_today']}\n\n"
        f"Stato: {status}\n\n"
        "🧪 Inviti reali: DISATTIVATI\n"
        "Questa versione serve a verificare la configurazione."
    )


# =========================================================
# START
# =========================================================

async def start(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not is_admin(update):
        await deny_access(update)
        return

    await update.message.reply_text(
        home_text(),
        reply_markup=main_keyboard(),
    )


# =========================================================
# CALLBACK
# =========================================================

async def buttons(update: Update, context: ContextTypes.DEFAULT_TYPE):

    if not is_admin(update):
        await deny_access(update)
        return

    query = update.callback_query
    await query.answer()

    data = query.data

    # -----------------------------------------------------
    # IMPOSTA A
    # -----------------------------------------------------

    if data == "set_a":

        state["waiting_for"] = "a"

        await query.edit_message_text(
            "📥 IMPOSTA GRUPPO A\n\n"
            "Scrivi il gruppo sorgente.\n\n"
            "Puoi usare:\n"
            "• @username\n"
            "• link t.me\n"
            "• ID Telegram\n\n"
            "Il bot controllerà esclusivamente il gruppo indicato.",
            reply_markup=InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(
                        "❌ ANNULLA",
                        callback_data="cancel_input",
                    )
                ]]
            ),
        )

    # -----------------------------------------------------
    # IMPOSTA B
    # -----------------------------------------------------

    elif data == "set_b":

        state["waiting_for"] = "b"

        await query.edit_message_text(
            "📤 IMPOSTA GRUPPO B\n\n"
            "Scrivi il gruppo destinazione.\n\n"
            "Puoi usare:\n"
            "• @username\n"
            "• link t.me\n"
            "• ID Telegram",
            reply_markup=InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(
                        "❌ ANNULLA",
                        callback_data="cancel_input",
                    )
                ]]
            ),
        )

    # -----------------------------------------------------
    # ANNULLA INPUT
    # -----------------------------------------------------

    elif data == "cancel_input":

        state["waiting_for"] = None

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    # -----------------------------------------------------
    # LIMITE -
    # -----------------------------------------------------

    elif data == "limit_minus":

        if state["daily_limit"] > 1:
            state["daily_limit"] -= 1

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    # -----------------------------------------------------
    # LIMITE +
    # -----------------------------------------------------

    elif data == "limit_plus":

        # Prima versione: massimo 5
        if state["daily_limit"] < 5:
            state["daily_limit"] += 1

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    # -----------------------------------------------------
    # AVVIA
    # -----------------------------------------------------

    elif data == "start_run":

        if not state["group_a"] or not state["group_b"]:

            await query.answer(
                "Imposta prima Gruppo A e Gruppo B.",
                show_alert=True,
            )
            return

        if state["group_a"]["id"] == state["group_b"]["id"]:

            await query.answer(
                "Gruppo A e Gruppo B non possono essere uguali.",
                show_alert=True,
            )
            return

        keyboard = InlineKeyboardMarkup(
            [
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
            ]
        )

        await query.edit_message_text(
            "⚠️ CONFERMA AVVIO\n\n"
            f"📥 Da: {state['group_a']['name']}\n"
            f"📤 A: {state['group_b']['name']}\n"
            f"🎯 Limite: {state['daily_limit']}/giorno\n\n"
            "🧪 In questa versione gli inviti reali "
            "sono ancora disattivati.",
            reply_markup=keyboard,
        )

    # -----------------------------------------------------
    # CONFERMA AVVIO
    # -----------------------------------------------------

    elif data == "confirm_run":

        state["running"] = True

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    # -----------------------------------------------------
    # PAUSA
    # -----------------------------------------------------

    elif data == "pause":

        state["running"] = False

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    # -----------------------------------------------------
    # STATO
    # -----------------------------------------------------

    elif data == "status":

        await query.edit_message_text(
            home_text(),
            reply_markup=InlineKeyboardMarkup(
                [[
                    InlineKeyboardButton(
                        "⬅️ INDIETRO",
                        callback_data="home",
                    )
                ]]
            ),
        )

    # -----------------------------------------------------
    # TEST CONFIG
    # -----------------------------------------------------

    elif data == "test_config":

        if not state["group_a"] or not state["group_b"]:

            await query.answer(
                "Imposta prima entrambi i gruppi.",
                show_alert=True,
            )
            return

        try:

            a = await user_client.get_entity(
                state["group_a"]["input"]
            )

            b = await user_client.get_entity(
                state["group_b"]["input"]
            )

            await query.edit_message_text(
                "✅ CONFIGURAZIONE VALIDA\n\n"
                f"📥 A: {getattr(a, 'title', 'Gruppo A')}\n"
                f"📤 B: {getattr(b, 'title', 'Gruppo B')}\n\n"
                "L'account operativo riesce ad accedere "
                "a entrambi.\n\n"
                "🧪 Nessun invito è stato effettuato.",
                reply_markup=InlineKeyboardMarkup(
                    [[
                        InlineKeyboardButton(
                            "⬅️ INDIETRO",
                            callback_data="home",
                        )
                    ]]
                ),
            )

        except Exception as e:

            logger.exception("Test configurazione fallito")

            await query.edit_message_text(
                "❌ TEST FALLITO\n\n"
                f"{type(e).__name__}\n\n"
                "Controlla i gruppi configurati.",
                reply_markup=InlineKeyboardMarkup(
                    [[
                        InlineKeyboardButton(
                            "⬅️ INDIETRO",
                            callback_data="home",
                        )
                    ]]
                ),
            )

    # -----------------------------------------------------
    # HOME
    # -----------------------------------------------------

    elif data == "home":

        state["waiting_for"] = None

        await query.edit_message_text(
            home_text(),
            reply_markup=main_keyboard(),
        )

    elif data == "noop":
        pass


# =========================================================
# INPUT TESTUALE PER GRUPPO A/B
# =========================================================

async def text_input(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE
):

    if not is_admin(update):
        await deny_access(update)
        return

    target = state["waiting_for"]

    if target not in ("a", "b"):
        return

    value = update.message.text.strip()

    try:

        group = await resolve_group(value)

        # Controllo A != B
        other = (
            state["group_b"]
            if target == "a"
            else state["group_a"]
        )

        if other and other["id"] == group["id"]:

            await update.message.reply_text(
                "❌ Questo gruppo è già impostato "
                "nell'altro campo.\n\n"
                "Gruppo A e Gruppo B devono essere diversi."
            )
            return

        if target == "a":
            state["group_a"] = group
        else:
            state["group_b"] = group

        state["waiting_for"] = None

        label = "A" if target == "a" else "B"

        await update.message.reply_text(
            f"✅ GRUPPO {label} IMPOSTATO\n\n"
            f"👥 {group['name']}\n"
            f"🆔 {group['id']}\n\n"
            "Il bot non ha enumerato gli altri gruppi "
            "dell'account.",
            reply_markup=main_keyboard(),
        )

    except Exception as e:

        logger.exception("Errore impostazione gruppo")

        await update.message.reply_text(
            "❌ Non riesco a utilizzare questo gruppo.\n\n"
            f"Errore: {type(e).__name__}\n\n"
            "Controlla @username/ID e assicurati che "
            "l'account operativo possa accedervi."
        )


# =========================================================
# TELETHON START/STOP
# =========================================================

async def post_init(application):

    logger.info("Connessione MTProto...")

    await user_client.connect()

    if not await user_client.is_user_authorized():
        raise RuntimeError(
            "TELEGRAM_SESSION non autorizzata."
        )

    me = await user_client.get_me()

    logger.info(
        "Account operativo collegato: %s (%s)",
        me.first_name,
        me.id,
    )


async def post_shutdown(application):

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

    logger.info("BestPrice Member Manager avviato.")

    application.run_polling()


if __name__ == "__main__":
    main()