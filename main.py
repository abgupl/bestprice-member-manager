
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
