import os
import asyncio
import logging
import math
import re
import html
import json
import secrets
import io
import struct
import zlib
from contextvars import ContextVar
from functools import wraps
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
import importlib.util

import aiosqlite

from telegram import Update, InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import RetryAfter
from telegram.ext import (
    Application,
    CommandHandler,
    CallbackQueryHandler,
    MessageHandler,
    ContextTypes,
    filters,
)

from telethon import TelegramClient, utils
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
    JoinChannelRequest,
)
from telethon.tl.functions.messages import CheckChatInviteRequest, ImportChatInviteRequest
from telethon.tl.functions.contacts import GetContactsRequest
from telethon.tl.functions.account import GetAuthorizationsRequest, CheckUsernameRequest, UpdateProfileRequest, UpdateUsernameRequest
from telethon.tl.functions.photos import UploadProfilePhotoRequest
from telethon.tl.types import Channel, Chat, User, ChannelParticipantsAdmins, UserStatusOnline, UserStatusOffline


# =========================================================
# CONFIG
# =========================================================

BOT_TOKEN = os.environ["BOT_TOKEN"]
API_ID = int(os.environ["API_ID"])
API_HASH = os.environ["API_HASH"]
TELEGRAM_SESSION = os.environ["TELEGRAM_SESSION"]
ADMIN_USER_ID = int(os.environ["ADMIN_USER_ID"])
# L'accesso al pannello è indipendente dagli account Telethon.
ADMIN_USER_IDS = {ADMIN_USER_ID}
if os.environ.get("ADMIN_USER_ID_2", "").strip():
    ADMIN_USER_IDS.add(int(os.environ["ADMIN_USER_ID_2"]))

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

ACCOUNT_IDS = tuple(range(1, 7))
session_clients = {}
session_info = {}
session_context = ContextVar("telegram_account", default=None)
control_lock = asyncio.Lock()
contact_task = None
welcome_bot = None
welcome_lock = asyncio.Lock()
welcome_tasks = set()

def account_suffix(account_id):
    return "" if account_id == 1 else f"_{account_id}"


def proxy_for_account(account_id):
    slot = int(os.environ.get(f"SESSION_PROXY_{account_id}", "1" if account_id <= 3 else "2"))
    if slot not in (1, 2):
        raise ValueError("SESSION_PROXY deve essere 1 oppure 2")
    host = os.environ.get(f"PROXY_{slot}_HOST", "").strip()
    if not host:
        raise ValueError(f"PROXY_{slot}_HOST mancante")
    port = int(os.environ.get(f"PROXY_{slot}_PORT", "50101"))
    if not 1 <= port <= 65535:
        raise ValueError("Porta proxy non valida")
    if importlib.util.find_spec("python_socks") is None:
        raise ValueError("Installa python-socks[asyncio] in requirements.txt")
    username = os.environ.get(f"PROXY_{slot}_USERNAME", "")
    password = os.environ.get(f"PROXY_{slot}_PASSWORD", "")
    if bool(username) != bool(password):
        raise ValueError("Username e password proxy devono essere entrambi presenti")
    return slot, {
        "proxy_type": "socks5", "addr": host, "port": port, "rdns": True,
        "username": username or None, "password": password or None,
    }


for account_id in ACCOUNT_IDS:
    suffix = account_suffix(account_id)
    session_string = os.environ.get(f"TELEGRAM_SESSION{suffix}", "").strip()
    info = {"ready": False, "name": f"ACCOUNT {account_id}", "user_id": None,
            "error": "Sessione non configurata", "telegram_locked": False,
            "telegram_restriction_detected": False, "proxy_slot": None}
    session_info[account_id] = info
    if session_string:
        try:
            slot, proxy = proxy_for_account(account_id)
            info["proxy_slot"] = slot
            options = {}
            for env_name, argument in (("DEVICE_MODEL", "device_model"),
                                       ("SYSTEM_VERSION", "system_version"),
                                       ("APP_VERSION", "app_version"),
                                       ("LANG_CODE", "lang_code"),
                                       ("SYSTEM_LANG_CODE", "system_lang_code")):
                value = os.environ.get(f"{env_name}{suffix}", "").strip()
                if value:
                    options[argument] = value
            session_clients[account_id] = TelegramClient(
                StringSession(session_string),
                int(os.environ.get(f"API_ID{suffix}") or API_ID),
                os.environ.get(f"API_HASH{suffix}") or API_HASH,
                proxy=proxy, flood_sleep_threshold=0, **options,
            )
            info["error"] = "Connessione da verificare"
        except Exception as exc:
            info["error"] = f"Configurazione non valida ({type(exc).__name__})"


def safe_connection_error(exc):
    message = str(exc)
    for slot in (1, 2):
        for field in ("PASSWORD", "USERNAME"):
            value = os.environ.get(f"PROXY_{slot}_{field}", "")
            if value:
                message = message.replace(value, "[riservato]")
    return f"{type(exc).__name__}: {message[:180]}"


def current_session_id():
    return session_context.get() or state["active_session"]


def session_label(account_id=None):
    account_id = account_id or current_session_id()
    return f"ACCOUNT {account_id} — {session_info[account_id]['name']}"


class SelectedClient:
    """Ogni task usa la sessione fissata all'avvio dell'operazione."""
    def _client(self):
        account_id = current_session_id()
        if not session_info[account_id]["ready"]:
            raise RuntimeError(f"ACCOUNT {account_id}: sessione non disponibile")
        return session_clients[account_id]

    def __getattr__(self, name):
        return getattr(self._client(), name)

    async def __call__(self, request):
        return await self._client()(request)


user_client = SelectedClient()


def operation_busy():
    return (state["running"] or state["contact_queue_running"]
            or state["manual_running"]
            or (worker_task is not None and not worker_task.done())
            or (contact_task is not None and not contact_task.done()))


def pinned_session(func):
    @wraps(func)
    async def wrapped(*args, **kwargs):
        token = session_context.set(current_session_id())
        try:
            return await func(*args, **kwargs)
        finally:
            session_context.reset(token)
    return wrapped


def serialized_control(func):
    @wraps(func)
    async def wrapped(*args, **kwargs):
        async with control_lock:
            token = session_context.set(state["active_session"])
            try:
                return await func(*args, **kwargs)
            finally:
                session_context.reset(token)
    return wrapped


# =========================================================
# STATO
# =========================================================

state = {
    "active_session": 1,
    "manual_running": False,
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

        cursor = await db.execute("PRAGMA table_info(logs)")
        if "session_id" not in {row[1] for row in await cursor.fetchall()}:
            await db.execute("ALTER TABLE logs ADD COLUMN session_id INTEGER NOT NULL DEFAULT 0")
        await db.execute("CREATE INDEX IF NOT EXISTS logs_session_id ON logs(session_id, id)")
        await db.execute("""
            CREATE TABLE IF NOT EXISTS session_contacts (
                session_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                username TEXT, display_name TEXT, phone TEXT, extracted_at TEXT NOT NULL,
                PRIMARY KEY (session_id, user_id)
            )
        """)
        cursor = await db.execute("SELECT value FROM settings WHERE key = 'contacts_sessions_migrated'")
        if not await cursor.fetchone():
            await db.execute("""
                INSERT OR IGNORE INTO session_contacts
                SELECT 1, user_id, username, display_name, phone, extracted_at
                FROM extracted_contacts
            """)
            await db.execute("INSERT INTO settings(key, value) VALUES ('contacts_sessions_migrated', '1')")

        await db.execute("""
            CREATE TABLE IF NOT EXISTS welcome_sent (
                chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                sent_at TEXT NOT NULL, message_id INTEGER NOT NULL,
                PRIMARY KEY(chat_id, user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS invitation_optout (
                chat_id INTEGER NOT NULL, user_id INTEGER NOT NULL, requested_at TEXT NOT NULL,
                PRIMARY KEY(chat_id, user_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS invitation_recovery (
                session_id INTEGER NOT NULL, user_id INTEGER NOT NULL,
                source_id INTEGER NOT NULL, destination_id INTEGER NOT NULL,
                attempted_at TEXT NOT NULL,
                PRIMARY KEY(session_id, user_id, source_id, destination_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS privacy_rejections (
                account_user_id INTEGER NOT NULL, target_user_id INTEGER NOT NULL,
                destination_id INTEGER NOT NULL, reason TEXT NOT NULL, detected_at TEXT NOT NULL,
                PRIMARY KEY(account_user_id, target_user_id, destination_id)
            )
        """)

        await db.execute("""
            CREATE TABLE IF NOT EXISTS membership_events (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_id INTEGER NOT NULL,
                user_id INTEGER NOT NULL, event TEXT NOT NULL, actor_id INTEGER,
                event_date TEXT, observed_at TEXT NOT NULL
            )
        """)
        await db.execute("CREATE INDEX IF NOT EXISTS membership_events_lookup ON membership_events(chat_id,user_id,id)")

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
    if key in {"telegram_locked", "telegram_restriction_detected"}:
        account_id = current_session_id()
        session_info[account_id][key] = str(value) == "1"
        key = f"session_{account_id}_{key}"

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

    state["interval_minutes"] = max(15, int(
        await get_setting(
            "interval_minutes",
            "10",
        )
    ))

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

    for account_id in ACCOUNT_IDS:
        for key in ("telegram_locked", "telegram_restriction_detected"):
            value = await get_setting(f"session_{account_id}_{key}")
            if value is None:
                value = await get_setting(key, "0") if account_id == 1 else "0"
                await set_setting(f"session_{account_id}_{key}", value)
            session_info[account_id][key] = value == "1"
    selected = await get_setting("active_session", "1")
    state["active_session"] = int(selected) if selected in {str(i) for i in ACCOUNT_IDS} else 1
    sync_session_state()

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
    session_id=None,
):
    account_id = current_session_id() if session_id is None else session_id
    if account_id:
        message = f"[{session_label(account_id)}] {message}"
    state["last_event"] = message

    async with aiosqlite.connect(DB_PATH) as db:

        await db.execute(
            """
            INSERT INTO logs(
                created_at,
                level,
                message,
                session_id
            )
            VALUES (?, ?, ?, ?)
            """,
            (
                now_it().isoformat(),
                level,
                message,
                account_id,
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


async def invitation_opted_out(user_id, chat_id=None):
    if chat_id is None:
        chat_id = int(state["group_b"]["id"])
        if chat_id > 0:
            chat_id = -1000000000000 - chat_id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM invitation_optout WHERE chat_id = ? AND user_id = ?",
            (chat_id, user_id),
        )
        return await cursor.fetchone() is not None


async def was_processed(user_id):
    if await invitation_opted_out(user_id):
        return True

    async with aiosqlite.connect(DB_PATH) as db:

        cursor = await db.execute(
            """
            SELECT status
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

    if row is None or row[0] in {"UNCONFIRMED", "VERIFY_PENDING"}:
        return False
    if row[0] == "NOT_ADDED":
        return await recovery_used(user_id)
    return True


async def processed_status(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT status FROM processed WHERE user_id = ? AND source_id = ? AND destination_id = ?",
            (user_id, state["group_a"]["id"], state["group_b"]["id"]),
        )
        row = await cursor.fetchone()
    return row[0] if row else None


async def recovery_used(user_id):
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT 1 FROM invitation_recovery WHERE session_id = ? AND user_id = ? AND source_id = ? AND destination_id = ?",
            (current_session_id(), user_id, state["group_a"]["id"], state["group_b"]["id"]),
        )
        return await cursor.fetchone() is not None


async def reserve_recovery(user_id):
    """Un solo nuovo tentativo di recupero per sessione, utente e coppia di gruppi."""
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "INSERT OR IGNORE INTO invitation_recovery VALUES (?, ?, ?, ?, ?)",
            (current_session_id(), user_id, state["group_a"]["id"], state["group_b"]["id"], now_it().isoformat()),
        )
        await db.commit()
        return cursor.rowcount == 1


def sync_session_state():
    info = session_info[state["active_session"]]
    state["telegram_locked"] = info["telegram_locked"]
    state["telegram_restriction_detected"] = info["telegram_restriction_detected"]


async def bind_session_owner(account_id, user_id):
    """Lo stato account segue l'identità Telegram, non il numero dello slot."""
    key = f"session_{account_id}_owner"
    previous = await get_setting(key)
    if previous == str(user_id):
        return
    # Primo binding della V4.7: le sessioni precedenti vengono sostituite.
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM session_contacts WHERE session_id = ?", (account_id,))
        await db.execute("DELETE FROM invitation_recovery WHERE session_id = ?", (account_id,))
        for flag in ("telegram_locked", "telegram_restriction_detected"):
            await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, '0')",
                             (f"session_{account_id}_{flag}",))
            session_info[account_id][flag] = False
        await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES (?, ?)", (key, str(user_id)))
        await db.execute("INSERT OR REPLACE INTO settings(key, value) VALUES ('auto_enabled', '0')")
        await db.commit()
    state["auto_enabled"] = False
    if account_id == state["active_session"]:
        sync_session_state()
    await add_log(f"👤 ACCOUNT {account_id} associato all'ID {user_id}; rubriche e blocchi locali del precedente slot azzerati. AUTO disattivato.", session_id=0)


async def connect_session(account_id):
    if account_id not in session_info:
        raise ValueError("Sessione non valida")
    info = session_info[account_id]
    client = session_clients.get(account_id)
    if client is None:
        return
    try:
        await asyncio.wait_for(client.connect(), timeout=30)
        if not await asyncio.wait_for(client.is_user_authorized(), timeout=15):
            raise ValueError("Sessione non autorizzata: rigenera la stringa")
        me = await asyncio.wait_for(client.get_me(), timeout=15)
        if me is None or getattr(me, "bot", False):
            raise ValueError("È necessario un account utente Telegram")
        for other_id, other in session_info.items():
            if other_id != account_id and other["ready"] and other["user_id"] == me.id:
                raise ValueError("Questo account è già presente in un altro slot: usa sei account distinti")
        await bind_session_owner(account_id, me.id)
        info.update(ready=True, user_id=me.id,
                    name=(f"@{me.username}" if me.username else me.first_name or str(me.id)),
                    error="Connessa e autorizzata")
    except Exception as exc:
        info["ready"] = False
        info["error"] = safe_connection_error(exc)
        logger.warning("ACCOUNT %s: %s", account_id, info["error"])
        await client.disconnect()


async def select_session(account_id):
    if account_id not in session_info:
        raise ValueError("Sessione non valida")
    if operation_busy():
        raise ValueError("Attendi la fine delle operazioni prima di cambiare sessione")
    if not session_info[account_id]["ready"]:
        raise ValueError("Sessione non disponibile: verifica connessione e variabili Railway")
    if account_id == state["active_session"]:
        return
    old_label = session_label(state["active_session"])
    # Il cambio richiede una nuova attivazione esplicita dell'automatico.
    state["auto_enabled"] = False
    await set_setting("auto_enabled", "0")
    state["active_session"] = account_id
    session_context.set(account_id)
    sync_session_state()
    await set_setting("active_session", account_id)
    state["waiting_for"] = None
    state["manual_ids"] = []
    state["manual_refs"] = []
    state["prepared_session"] = None
    state["member_page"] = 0
    state["contact_page"] = 0
    await add_log(f"👤 Sessione selezionata — precedente: {old_label}. Automatico disattivato: riattivalo esplicitamente.")


async def sessions_text():
    lines = ["👤 GESTIONE SESSIONI", f"Attiva: {session_label(state['active_session'])}", ""]
    for account_id in ACCOUNT_IDS:
        info = session_info[account_id]
        lines.extend([session_label(account_id),
                      f"ID: {info['user_id'] or 'non disponibile'}",
                      f"🔌 {info['error']}",
                      f"🌐 Proxy: {info['proxy_slot'] or 'non configurato'} (SOCKS5)",
                      "🔒 Inviti bloccati localmente" if info["telegram_locked"] else "🔓 Nessun blocco locale", ""])
    lines.append("La verifica controlla l'accesso alla sessione, non l'assenza di limitazioni Telegram.\n"
                 "Il cambio disattiva la programmazione AUTO e annulla le liste preparate.")
    return "\n".join(lines)


def sessions_keyboard():
    rows = []
    for account_id in ACCOUNT_IDS:
        rows.append([InlineKeyboardButton(
            f"{'✅' if account_id == state['active_session'] else '👤'} USA ACCOUNT {account_id}",
            callback_data=f"select_session:{account_id}"),
            InlineKeyboardButton("🔎 VERIFICA", callback_data=f"check_session:{account_id}")])
        rows.append([InlineKeyboardButton(f"🎲 GENERA PROFILO — ACCOUNT {account_id}", callback_data=f"profile_new:{account_id}")])
    rows.append([InlineKeyboardButton("📥 TUTTE LE SESSIONI NEL GRUPPO A", callback_data="join_a_setup")])
    rows.append([InlineKeyboardButton("🌐 PROXY / DIAGNOSTICA", callback_data="proxy_status")])
    rows.append([InlineKeyboardButton("⬅️ HOME", callback_data="home")])
    return InlineKeyboardMarkup(rows)


PROFILE_COLORS = ((211, 45, 55), (25, 112, 182), (32, 143, 106), (111, 67, 174), (202, 112, 26), (35, 127, 145))
PROFILE_NAMES = ("Nuvio", "Zampix", "Lunetto", "Puffolo", "Morbix", "Tondino", "Frullix", "Baffolo", "Piumix", "Nebulino", "Zuffolo", "Brillix")
PROFILE_BIOS = ("Mascotte della community. Account gestito dal team.",
                "Account di gestione della community con avatar mascotte.",
                "Mascotte virtuale del team. Supporto alla community.")
PROFILE_MASCOTS = ("orsetto", "gattino", "gufetto")

def profile_avatar(account_id, color, mascot="orsetto"):
    """Mascotte illustrata + slot, PNG senza dipendenze esterne."""
    size = 512
    pixels = bytearray(bytes(color) * (size * size))
    def rect(x, y, w, h, rgb):
        for row in range(max(0, y), min(size, y + h)):
            start = (row * size + max(0, x)) * 3
            end = (row * size + min(size, x + w)) * 3
            pixels[start:end] = bytes(rgb) * ((end - start) // 3)
    glyphs = {
        'B': ('11110','10001','10001','11110','10001','10001','11110'),
        '0': ('01110','10001','10011','10101','11001','10001','01110'),
        '1': ('00100','01100','00100','00100','00100','00100','01110'),
        '2': ('01110','10001','00001','00010','00100','01000','11111'),
        '3': ('11110','00001','00001','01110','00001','00001','11110'),
        '4': ('00010','00110','01010','10010','11111','00010','00010'),
        '5': ('11111','10000','10000','11110','00001','00001','11110'),
        '6': ('01110','10000','10000','11110','10001','10001','01110')}
    def text(value, y, scale):
        x = (size - (len(value) * 6 - 1) * scale) // 2
        for char in value:
            for row, bits in enumerate(glyphs[char]):
                for col, bit in enumerate(bits):
                    if bit == '1': rect(x + col * scale, y + row * scale, scale, scale, (255,255,255))
            x += 6 * scale
    def ellipse(cx, cy, rx, ry, rgb):
        for y in range(max(0, cy - ry), min(size, cy + ry + 1)):
            half = int(rx * math.sqrt(max(0, 1 - ((y - cy) / ry) ** 2)))
            rect(cx - half, y, half * 2 + 1, 1, rgb)
    dark = (38, 43, 63)
    cream = (255, 240, 218)
    peach = (242, 179, 166)
    ellipse(256, 265, 193, 193, tuple(min(255, c + 26) for c in color))
    if mascot == 'gattino':
        # Orecchie a punta, disegnate a righe.
        for y in range(90, 210):
            width = (y - 90) // 2
            rect(155 - width, y, width * 2 + 1, 1, cream)
            rect(357 - width, y, width * 2 + 1, 1, cream)
    elif mascot == 'orsetto':
        ellipse(151, 166, 64, 66, cream)
        ellipse(361, 166, 64, 66, cream)
        ellipse(151, 166, 35, 37, peach)
        ellipse(361, 166, 35, 37, peach)
    else:
        ellipse(169, 195, 79, 82, cream)
        ellipse(343, 195, 79, 82, cream)
    ellipse(256, 270, 148, 132, cream)
    if mascot == 'gufetto':
        ellipse(194, 247, 53, 57, (255,255,255))
        ellipse(318, 247, 53, 57, (255,255,255))
    ellipse(198, 247, 16, 24, dark)
    ellipse(314, 247, 16, 24, dark)
    ellipse(194, 239, 5, 7, (255,255,255))
    ellipse(310, 239, 5, 7, (255,255,255))
    ellipse(161, 292, 22, 13, peach)
    ellipse(351, 292, 22, 13, peach)
    ellipse(256, 286, 17, 12, (229,148,55) if mascot == 'gufetto' else dark)
    rect(253, 297, 6, 18, dark)
    ellipse(241, 313, 17, 5, dark)
    ellipse(271, 313, 17, 5, dark)
    if mascot == 'gattino':
        rect(112, 276, 46, 4, dark)
        rect(111, 299, 46, 4, dark)
        rect(354, 276, 46, 4, dark)
        rect(355, 299, 46, 4, dark)
    ellipse(256, 416, 65, 47, dark)
    text(str(account_id).zfill(2), 389, 8)
    def chunk(kind, payload):
        return struct.pack('>I', len(payload)) + kind + payload + struct.pack('>I', zlib.crc32(kind + payload) & 0xffffffff)
    raw = b''.join(b'\x00' + pixels[row*size*3:(row+1)*size*3] for row in range(size))
    return b'\x89PNG\r\n\x1a\n' + chunk(b'IHDR', struct.pack('>IIBBBBB',size,size,8,2,0,0,0)) + chunk(b'IDAT',zlib.compress(raw)) + chunk(b'IEND',b'')

async def profile_action(update, context):
    query = update.callback_query
    data = query.data
    if data == 'profile_cancel':
        context.user_data.pop('profile_pending', None)
        await query.edit_message_text('Profilo annullato.', reply_markup=sessions_keyboard())
        return
    if operation_busy() or state['auto_enabled']:
        await callback_notice(query, 'Disattiva AUTO e attendi la fine delle operazioni prima di modificare i profili.')
        return
    if data.startswith('profile_new:') or data.startswith('profile_regen:'):
        if data.startswith('profile_regen:'):
            old = context.user_data.get('profile_pending')
            if not old or old['nonce'] != data.split(':',1)[1]:
                await callback_notice(query, 'Anteprima scaduta: riapri GENERA PROFILO.')
                return
            account_id = old['account_id']
        else:
            value = data.split(':',1)[1]
            account_id = int(value) if value.isdigit() else 0
        context.user_data.pop('profile_pending', None)
        if account_id not in ACCOUNT_IDS or not session_info[account_id]['ready']:
            await callback_notice(query, 'Sessione non disponibile: usa VERIFICA.')
            return
        if await get_setting(f'profile_pause_{account_id}', '0') and float(await get_setting(f'profile_pause_{account_id}', '0')) > now_it().timestamp():
            await callback_notice(query, 'Telegram ha richiesto una pausa per il profilo di questo account. Attendi prima di riprovare.')
            return
        client = session_clients[account_id]
        try:
            me = await asyncio.wait_for(client.get_me(), 15)
            if not me or me.id != session_info[account_id]['user_id']:
                raise ValueError('Identità della sessione cambiata: esegui VERIFICA')
            role = secrets.choice(PROFILE_NAMES)
            username = f'{role.lower()}_community_{secrets.token_hex(3)}'
            available = bool(await asyncio.wait_for(client(CheckUsernameRequest(username)), 15))
            pending = {'account_id': account_id, 'owner_id': me.id,
                       'admin_id': update.effective_user.id, 'created_at': now_it().timestamp(),
                       'nonce': secrets.token_hex(6), 'first_name': role,
                       'last_name': '', 'bio': secrets.choice(PROFILE_BIOS),
                       'username': username if available else None,
                       'color': secrets.choice(PROFILE_COLORS), 'mascot': secrets.choice(PROFILE_MASCOTS)}
            avatar = profile_avatar(account_id, pending['color'], pending['mascot'])
            caption = (f"🎲 ANTEPRIMA ACCOUNT {account_id} — ID {me.id}\n\n"
                       f"Nome: {pending['first_name']}\nAvatar: {pending['mascot']}\n"
                       f"Bio: {pending['bio']}\n"
                       + (f"Username: @{username} — disponibile al controllo" if available else 'Username proposto non disponibile: quello attuale sarà mantenuto')
                       + '\n\nProfilo del progetto. Modifiche solo dopo APPLICA; nessun invito.')
            await query.message.reply_photo(photo=avatar, caption=caption)
            nonce = pending['nonce']
            await query.edit_message_text(f'Anteprima pronta per ACCOUNT {account_id}. APPLICA sostituirà nome, bio, foto e lo username disponibile. La vecchia foto resterà nello storico Telegram.',
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton('🔄 RIGENERA', callback_data=f'profile_regen:{nonce}'), InlineKeyboardButton('✅ APPLICA', callback_data=f'profile_apply:{nonce}')],
                    [InlineKeyboardButton('❌ ANNULLA', callback_data='profile_cancel')]]))
            context.user_data['profile_pending'] = pending
        except Exception as exc:
            if isinstance(exc, FloodWaitError):
                await set_setting(f'profile_pause_{account_id}', now_it().timestamp() + exc.seconds)
            await add_log('👤 ANTEPRIMA PROFILO FALLITA — ' + safe_connection_error(exc), session_id=account_id)
            await callback_notice(query, 'Anteprima non creata — ' + safe_connection_error(exc))
        return
    pending = context.user_data.get('profile_pending')
    if (not pending or pending['nonce'] != data.split(':',1)[1]
            or pending['admin_id'] != update.effective_user.id
            or now_it().timestamp() - pending['created_at'] > 600):
        await callback_notice(query, 'Anteprima scaduta o già utilizzata: genera un nuovo profilo.')
        return
    account_id = pending['account_id']
    client = session_clients[account_id]
    context.user_data.pop('profile_pending', None)  # nessuna replica tramite doppio clic
    lines = [f'👤 AGGIORNAMENTO PROFILO — ACCOUNT {account_id}']
    try:
        me = await asyncio.wait_for(client.get_me(), 15)
        if not session_info[account_id]['ready'] or not me or me.id != pending['owner_id']:
            raise ValueError('Identità della sessione cambiata: nessuna modifica eseguita')
        pause_until = float(await get_setting(f'profile_pause_{account_id}', '0'))
        if pause_until > now_it().timestamp():
            raise ValueError('Pausa Telegram ancora attiva per questo profilo')
        await asyncio.wait_for(client(UpdateProfileRequest(first_name=pending['first_name'], last_name=pending['last_name'], about=pending['bio'])), 20)
        lines.append('✅ Nome e bio aggiornati')
        await add_log('👤 PROFILO — nome e bio aggiornati', session_id=account_id)
        session_info[account_id]['name'] = f"@{me.username}" if me.username else (pending['first_name'] + ' ' + pending['last_name']).strip()
        if pending['username']:
            await asyncio.wait_for(client(UpdateUsernameRequest(pending['username'])), 20)
            session_info[account_id]['name'] = '@' + pending['username']
            lines.append('✅ Username aggiornato: @' + pending['username'])
            await add_log('👤 PROFILO — username aggiornato', session_id=account_id)
        else:
            lines.append('ℹ️ Username attuale mantenuto')
        photo = io.BytesIO(profile_avatar(account_id, pending['color'], pending['mascot']))
        photo.name = f'mascotte_account_{account_id}.png'
        uploaded = await asyncio.wait_for(client.upload_file(photo), 30)
        await asyncio.wait_for(client(UploadProfilePhotoRequest(file=uploaded)), 20)
        lines.append('✅ Avatar aggiornato')
        await add_log('👤 PROFILO — avatar aggiornato su conferma amministratore', session_id=account_id)
    except Exception as exc:
        if isinstance(exc, FloodWaitError):
            await set_setting(f'profile_pause_{account_id}', now_it().timestamp() + exc.seconds)
        lines.append('⚠️ Operazione interrotta — ' + safe_connection_error(exc))
        lines.append('Le modifiche già riuscite restano applicate. In caso di timeout verifica il profilo su Telegram prima di riprovare.')
        await add_log('👤 PROFILO INTERROTTO — ' + safe_connection_error(exc), session_id=account_id)
    await query.edit_message_text('\n'.join(lines), reply_markup=sessions_keyboard())


proxy_checks = {}
ip_checks = {}


def proxy_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("🧪 TEST PROXY 1", callback_data="proxy_test:1"),
         InlineKeyboardButton("🧪 TEST PROXY 2", callback_data="proxy_test:2")],
        [InlineKeyboardButton("🔎 IP TELEGRAM — SESSIONE ATTIVA", callback_data="proxy_ip")],
        [InlineKeyboardButton("🔄 AGGIORNA", callback_data="proxy_status")],
        [InlineKeyboardButton("👤 SESSIONI", callback_data="sessions")],
        [InlineKeyboardButton("⬅️ HOME", callback_data="home")],
    ])


async def proxy_status_text():
    lines = ["🌐 PROXY / DIAGNOSTICA", ""]
    for slot in (1, 2):
        host = os.environ.get(f"PROXY_{slot}_HOST", "").strip()
        port = os.environ.get(f"PROXY_{slot}_PORT", "50101")
        accounts = [str(i) for i in ACCOUNT_IDS if session_info[i]["proxy_slot"] == slot]
        lines.extend([f"PROXY {slot} — SOCKS5 {host or 'non configurato'}:{port}",
                      "Account configurati: " + (", ".join(accounts) or "nessuno"),
                      proxy_checks.get(slot, "⚪ Non ancora testato in questo avvio"), ""])
    account_id = state["active_session"]
    lines.extend(["Sessione attiva: " + session_label(account_id),
                  ip_checks.get(account_id, "⚪ IP visto da Telegram non ancora verificato"), "",
                  "Il test verifica autenticazione SOCKS5 e apertura di una connessione verso Telegram.",
                  "L'IP è quello riportato da Telegram per l'autorizzazione corrente: può essere aggiornato con ritardo.",
                  "Un risultato positivo non certifica l'assenza di limitazioni antispam.",
                  "Questi controlli non inviano inviti e non rimuovono i blocchi locali."])
    return "\n".join(lines)


async def test_proxy_connection(slot):
    if slot not in (1, 2):
        raise ValueError("Proxy non valido")
    sock = None
    stamp = now_it().strftime("%H:%M:%S")
    try:
        from python_socks import ProxyType
        from python_socks.async_.asyncio import Proxy
        candidates = [i for i in ACCOUNT_IDS if session_info[i]["proxy_slot"] == slot]
        if not candidates:
            raise ValueError("Nessuna sessione configurata per questo proxy")
        account_id = candidates[0]
        _, config = proxy_for_account(account_id)
        client = session_clients.get(account_id)
        if client is None:
            raise ValueError("Client della sessione non disponibile")
        target = client.session.server_address
        target_port = client.session.port
        proxy = Proxy(proxy_type=ProxyType.SOCKS5, host=config["addr"], port=config["port"],
                      username=config["username"], password=config["password"], rdns=True)
        started = asyncio.get_running_loop().time()
        sock = await asyncio.wait_for(proxy.connect(dest_host=target, dest_port=target_port, timeout=10), timeout=12)
        elapsed = int((asyncio.get_running_loop().time() - started) * 1000)
        result = f"✅ {stamp} — SOCKS5 autenticato; connessione verso Telegram aperta ({elapsed} ms)"
    except Exception as exc:
        result = f"❌ {stamp} — TEST FALLITO — {safe_connection_error(exc)}"
    finally:
        if sock is not None:
            sock.close()
    proxy_checks[slot] = result
    await add_log(f"🌐 PROXY {slot} — {result}", session_id=0)
    return result


async def check_telegram_ip(account_id):
    stamp = now_it().strftime("%H:%M:%S")
    info = session_info[account_id]
    if not info["ready"]:
        result = f"⚠️ {stamp} — Sessione non connessa: usa VERIFICA nel menu SESSIONI"
    else:
        try:
            response = await asyncio.wait_for(session_clients[account_id](GetAuthorizationsRequest()), timeout=15)
            current = next((a for a in response.authorizations if getattr(a, "current", False)), None)
            if current is None or not getattr(current, "ip", None):
                raise ValueError("IP dell'autorizzazione corrente non disponibile")
            slot = info["proxy_slot"]
            expected = os.environ.get(f"PROXY_{slot}_HOST", "").strip()
            result = f"🔎 {stamp} — IP riportato da Telegram: {current.ip}"
            if current.ip == expected:
                result += f" — coincide con il server proxy {slot}"
            else:
                result += " — diverso dal server proxy configurato; può essere un IP di uscita distinto o un dato non ancora aggiornato"
        except Exception as exc:
            result = f"❌ {stamp} — VERIFICA IP FALLITA — {safe_connection_error(exc)}"
    ip_checks[account_id] = result
    await add_log(result, session_id=account_id)
    return result


async def diagnostic_group(client, group, label):
    if not group:
        return [f"📂 {label}: non configurato"]
    lines = [f"📂 {label}: {str(group.get('name', group['id']))[:80]}"]
    try:
        entity = await asyncio.wait_for(client.get_entity(group["input"]), timeout=10)
        permissions = await asyncio.wait_for(client.get_permissions(entity, "me"), timeout=10)
        if permissions is None:
            lines.append("⚠️ Permessi personali non disponibili")
        elif permissions.has_left or getattr(getattr(getattr(permissions, "participant", None), "banned_rights", None), "view_messages", False):
            lines.append("❌ Non membro oppure escluso dal gruppo")
        else:
            role = "proprietario" if permissions.is_creator else "amministratore" if permissions.is_admin else "membro"
            lines.append(f"✅ Presenza verificata — ruolo: {role}")
            if permissions.is_creator or permissions.is_admin:
                allowed = bool(permissions.is_creator or permissions.invite_users)
            else:
                personal = getattr(getattr(permissions, "participant", None), "banned_rights", None)
                defaults = getattr(entity, "default_banned_rights", None)
                allowed = not (getattr(personal, "invite_users", False) or getattr(defaults, "invite_users", False))
            lines.append("Invita utenti (permessi del gruppo): " + ("✅ consentito" if allowed else "❌ non consentito"))
            if permissions.is_banned:
                lines.append("⚠️ Account con restrizioni personali nel gruppo")
            if label == "GRUPPO B" and not allowed:
                lines.append("Azione: controlla i permessi del gruppo per questo account.")
        if getattr(entity, "participants_count", None) is not None:
            lines.append(f"Membri riportati: {entity.participants_count}")
    except Exception as exc:
        if type(exc).__name__ == "UserNotParticipantError":
            lines.append("ℹ️ Account non membro: la lettura dipende dall'accesso consentito da Telegram." if label == "GRUPPO A" else "❌ Account non membro: deve prima entrare nel gruppo.")
        else:
            lines.append("⚠️ Accesso/permessi non verificabili — " + safe_connection_error(exc))
    return lines


def classify_spambot_reply(reply):
    """Riconosce soltanto risposte esplicite; non deduce uno stato da una parola."""
    text = " ".join(reply.lower().replace("’", "'").split())
    clear = any(phrase in text for phrase in (
        "good news, no limits are currently applied to your account",
        "nessuna limitazione è attualmente applicata al tuo account",
        "nessun limite è attualmente applicato al tuo account",
    ))
    limited = any(phrase in text for phrase in (
        "your account is now limited", "your account is limited",
        "your account has been limited", "your account will be automatically released",
        "il tuo account è attualmente limitato", "il tuo account è stato limitato",
    ))
    if clear and not limited:
        return "NO_LIMITS_REPORTED"
    if limited and not clear:
        return "LIMITED"
    return "UNKNOWN"


async def spambot_record(account_id):
    owner = session_info[account_id]["user_id"]
    if not owner:
        return {}
    try:
        record = json.loads(await get_setting(f"spambot_check_{owner}", "{}"))
        return record if isinstance(record, dict) else {}
    except (ValueError, TypeError):
        return {}


async def query_spambot(account_id):
    """Richiesta esplicita dal pannello; nessun appello, invito o sblocco automatico."""
    info = session_info[account_id]
    if not info["ready"] or not info["user_id"]:
        return
    record = await spambot_record(account_id)
    attempted = now_it().timestamp()
    if attempted - float(record.get("attempted_at", 0)) < 60:
        return  # Il report mantiene l'ora originale: non presenta una cache come nuova risposta.
    record = {"status": "UNKNOWN", "attempted_at": attempted,
              "checked_at": "", "reply": "", "error": "Risposta non ancora ricevuta"}
    key = f"spambot_check_{info['user_id']}"
    await set_setting(key, json.dumps(record, ensure_ascii=False))
    client = session_clients[account_id]
    try:
        entity = await asyncio.wait_for(client.get_entity("@SpamBot"), timeout=10)
        if not getattr(entity, "bot", False) or (getattr(entity, "username", None) or "").lower() != "spambot":
            raise ValueError("Identità di @SpamBot non verificata")
        async with client.conversation(entity, timeout=20, total_timeout=25, exclusive=True) as conversation:
            sent = await conversation.send_message("/start")
            response = await conversation.get_response(sent)
        if (getattr(response, "sender_id", None) != entity.id
                or getattr(response, "out", False)
                or response.id <= sent.id):
            raise ValueError("Risposta non valida o non successiva alla richiesta")
        reply = getattr(response, "message", None) or ""
        record.update(status=classify_spambot_reply(reply), checked_at=now_it().isoformat(),
                      reply=reply[:2000], error="")
        if record["status"] == "LIMITED":
            for flag in ("telegram_locked", "telegram_restriction_detected"):
                info[flag] = True
                await set_setting(f"session_{account_id}_{flag}", "1")
            if account_id == state["active_session"]:
                sync_session_state()
                state["auto_enabled"] = False
                await set_setting("auto_enabled", "0")
        await add_log("🤖 SPAMBOT — verifica aggiornata: " + record["status"] +
                      "; blocco locale mantenuto se già presente", session_id=account_id)
    except Exception as exc:
        record["error"] = ("Nessuna risposta entro il tempo previsto" if isinstance(exc, asyncio.TimeoutError)
                           else safe_connection_error(exc))
        await add_log("⚠️ SPAMBOT — stato non determinabile: " + record["error"], session_id=account_id)
    await set_setting(key, json.dumps(record, ensure_ascii=False))


async def spambot_report(account_id):
    record = await spambot_record(account_id)
    lines = ["", "🤖 SPAMBOT — controllo su richiesta con /start"]
    if not record:
        return lines + ["Stato non determinabile: nessun controllo disponibile per questo account."]
    stamp = datetime.fromtimestamp(record["attempted_at"], ITALY_TZ).strftime("%d/%m/%Y %H:%M:%S")
    lines.append("Ultima richiesta: " + stamp + " (richieste distanziate di almeno 60 secondi)")
    labels = {"LIMITED": "🔒 LIMITATO secondo @SpamBot",
              "NO_LIMITS_REPORTED": "✅ NESSUNA LIMITAZIONE SEGNALATA da @SpamBot",
              "UNKNOWN": "⚠️ STATO NON DETERMINABILE"}
    lines.append(labels.get(record.get("status"), labels["UNKNOWN"]))
    if record.get("checked_at"):
        lines.append("Risposta ricevuta: " + datetime.fromisoformat(record["checked_at"]).astimezone(ITALY_TZ).strftime("%d/%m/%Y %H:%M:%S"))
    if record.get("reply"):
        lines.append("Testo di @SpamBot: " + record["reply"])
    if record.get("error"):
        lines.append("Dettaglio: " + record["error"])
    lines.append("Lo stato di @SpamBot non certifica che gli inviti siano consentiti. Nessun blocco precedente viene rimosso automaticamente.")
    return lines


async def session_diagnostic_text(account_id):
    info = session_info[account_id]
    lines = [f"🔎 VERIFICA DETTAGLIATA — ACCOUNT {account_id}",
             "Controllo: " + now_it().strftime("%d/%m/%Y %H:%M:%S"),
             "Connessione: " + info["error"],
             "ID Telegram: " + str(info["user_id"] or "non disponibile"),
             "Blocco locale antispam: " + ("🔒 ATTIVO" if info["telegram_locked"] else "nessuno registrato"), ""]
    slot = info["proxy_slot"]
    if slot:
        lines.append(f"🌐 Proxy assegnato: {slot} — SOCKS5")
        await test_proxy_connection(slot)
        lines.append(proxy_checks[slot])
    else:
        lines.append("❌ Proxy non configurato: controlla le variabili Railway.")
    if info["ready"]:
        client = session_clients[account_id]
        try:
            me = await asyncio.wait_for(client.get_me(), timeout=10)
            if me is None:
                raise ValueError("Identita account non disponibile")
            lines.extend(["", "👤 IDENTITÀ",
                          "Nome: " + " ".join(x for x in (getattr(me, "first_name", None), getattr(me, "last_name", None)) if x)[:100],
                          "Username: " + ("@" + me.username if getattr(me, "username", None) else "non impostato"),
                          "Numero: " + ("+" + me.phone if getattr(me, "phone", None) else "non disponibile"),
                          "Premium: " + ("sì" if getattr(me, "premium", False) else "no"),
                          "Account eliminato: " + ("sì" if getattr(me, "deleted", False) else "no"),
                          "Flag restricted: " + ("sì" if getattr(me, "restricted", False) else "no"),
                          "Flag scam/fake: " + ("sì" if getattr(me, "scam", False) or getattr(me, "fake", False) else "no")])
            reasons = getattr(me, "restriction_reason", None) or []
            for reason in reasons[:2]:
                lines.append("Restrizione contenuti: " + str(getattr(reason, "text", "non specificata"))[:160])
            lines.append("I flag dell'identità non certificano l'assenza di limiti sugli inviti.")
        except Exception as exc:
            lines.append("⚠️ Lettura identità fallita — " + safe_connection_error(exc))
        lines.extend(["", "🌐 CONNESSIONE DELLA SESSIONE"])
        lines.append(await check_telegram_ip(account_id))
        lines.append("L'IP riportato può aggiornarsi con ritardo o differire dall'IP del server proxy.")
        suffix = account_suffix(account_id)
        for name, label in (("DEVICE_MODEL", "Dispositivo configurato"), ("SYSTEM_VERSION", "Sistema"), ("APP_VERSION", "Versione client")):
            lines.append(label + ": " + os.environ.get(name + suffix, "predefinito Telethon")[:80])
        for key, label in (("group_a", "GRUPPO A"), ("group_b", "GRUPPO B")):
            lines.append("")
            lines.extend(await diagnostic_group(client, state[key], label))
    lines.extend(await spambot_report(account_id))
    owner = info["user_id"] or ('slot_' + str(account_id))
    deadline = float(await get_setting(f"auto_next_invite_{owner}", "0"))
    remaining = max(0, deadline - now_it().timestamp())
    lines.extend(["", "⏱ Pausa AUTO residua: " + (f"circa {math.ceil(remaining / 60)} minuti" if remaining else "nessuna")])
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute(
            "SELECT created_at, message FROM logs WHERE session_id = ? AND "
            "(message LIKE '%PeerFlood%' OR message LIKE '%FloodWait%' OR message LIKE '%PROBLEMA GRUPPO B%') ORDER BY id DESC LIMIT 1",
            (account_id,),
        )
        row = await cursor.fetchone()
    if row:
        lines.extend(["", "📋 Ultimo errore rilevante registrato: " + row[0], row[1][:450]])
    lines.extend(["", "⚠️ Nessun test d'invito eseguito. Telegram non fornisce qui una certificazione preventiva dell'assenza di limiti antispam.",
                  "PeerFlood indica una limitazione sugli inviti, non necessariamente un ban completo. Non rimuovere il blocco per riprovare."])
    return "\n".join(lines)


def diagnostic_keyboard():
    return InlineKeyboardMarkup([
        [InlineKeyboardButton("👤 SESSIONI", callback_data="sessions")],
        [InlineKeyboardButton("🌐 PROXY", callback_data="proxy_status")],
        [InlineKeyboardButton("⬅️ HOME", callback_data="home")],
    ])


async def myid(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if update.effective_user and update.effective_message:
        await update.effective_message.reply_text(f"Il tuo ID Telegram è: {update.effective_user.id}")


# =========================================================
# SICUREZZA BOT
# =========================================================

def is_admin(update):

    user = update.effective_user

    return (
        user is not None
        and user.id in ADMIN_USER_IDS
    )


async def deny_access(update):

    if update.callback_query:

        await update.callback_query.answer(
            "⛔ Accesso non autorizzato al pannello. Usa /myid per conoscere il tuo ID.",
            show_alert=True,
        )

    elif update.effective_message:

        await update.effective_message.reply_text(
            "⛔ Accesso non autorizzato al pannello. Usa /myid per conoscere il tuo ID."
        )


# =========================================================
# GRUPPI
# =========================================================

def parse_join_reference(value):
    value = value.strip()
    private = re.fullmatch(r"(?:https?://)?(?:t\.me|telegram\.me)/(?:\+|joinchat/)([A-Za-z0-9_-]+)(?:\?[^\s]*)?/?", value)
    if private:
        return {"private": True, "value": private.group(1), "display": value}
    public = re.fullmatch(r"(?:https?://)?(?:t\.me|telegram\.me)/([A-Za-z][A-Za-z0-9_]{3,})(?:/)?", value)
    username = public.group(1) if public else value.lstrip("@")
    if not re.fullmatch(r"[A-Za-z][A-Za-z0-9_]{3,}", username):
        raise ValueError("Inserisci @username o un link t.me valido del gruppo A, non il solo ID.")
    return {"private": False, "value": "@" + username, "display": value}


async def inspect_join_target(client, reference, group):
    if reference["private"]:
        invite = await asyncio.wait_for(client(CheckChatInviteRequest(reference["value"])), timeout=15)
        entity = getattr(invite, "chat", None)
        title = getattr(entity or invite, "title", "")
        if entity is not None:
            if entity.id != group["id"]:
                raise ValueError("Il link appartiene a un gruppo diverso dal gruppo A.")
        elif title != group["name"]:
            raise ValueError("Il nome del gruppo nel link non corrisponde al gruppo A configurato.")
        if getattr(invite, "broadcast", False) or getattr(entity, "broadcast", False):
            raise ValueError("Il link appartiene a un canale, non a un gruppo.")
        return entity, title, entity is not None
    entity = await asyncio.wait_for(client.get_entity(reference["value"]), timeout=15)
    if not isinstance(entity, Channel) or not entity.megagroup or entity.id != group["id"]:
        raise ValueError("Lo username non appartiene al gruppo A configurato.")
    return entity, entity.title, True


async def join_source_account(account_id, reference, group):
    info = session_info[account_id]
    if not info["ready"]:
        await connect_session(account_id)
    if not info["ready"]:
        return "❌ Sessione non disponibile — " + info["error"], False
    owner = info["user_id"]
    deadline_key = f"join_source_wait_{owner}"
    remaining = float(await get_setting(deadline_key, "0")) - now_it().timestamp()
    if remaining > 0:
        return f"⏳ Pausa Telegram ancora attiva: {math.ceil(remaining)} secondi", True
    client = session_clients[account_id]
    try:
        entity, _, _ = await inspect_join_target(client, reference, group)
        if entity is not None:
            try:
                permissions = await asyncio.wait_for(client.get_permissions(entity, "me"), timeout=15)
                if permissions is not None and not permissions.has_left and not getattr(getattr(getattr(permissions, "participant", None), "banned_rights", None), "view_messages", False):
                    return "↪️ Già membro del gruppo A", False
            except Exception as exc:
                if type(exc).__name__ != "UserNotParticipantError":
                    raise
        request = ImportChatInviteRequest(reference["value"]) if reference["private"] else JoinChannelRequest(entity)
        result = await asyncio.wait_for(client(request), timeout=20)
        # Per link privati senza ID nella preview, usa il gruppo restituito dalla risposta.
        if reference["private"]:
            entity = next((chat for chat in getattr(result, "chats", []) if getattr(chat, "id", None) == group["id"]), None)
            if entity is None:
                raise ValueError("Risposta ricevuta, ma gruppo A non identificato: esito da verificare, nessun nuovo tentativo automatico.")
        permissions = await asyncio.wait_for(client.get_permissions(entity, "me"), timeout=15)
        if permissions is None or permissions.has_left or getattr(getattr(getattr(permissions, "participant", None), "banned_rights", None), "view_messages", False):
            return "⚠️ Richiesta ricevuta, presenza non confermata", False
        return "✅ Entrato nel gruppo A — presenza verificata", False
    except FloodWaitError as exc:
        await set_setting(deadline_key, str(now_it().timestamp() + exc.seconds))
        return f"⏳ Telegram richiede una pausa di {exc.seconds} secondi; operazione interrotta", True
    except PeerFloodError:
        for flag in ("telegram_locked", "telegram_restriction_detected"):
            info[flag] = True
            await set_setting(f"session_{account_id}_{flag}", "1")
        if account_id == state["active_session"]:
            sync_session_state()
        return "🛑 PeerFloodError: limitazione Telegram; operazione interrotta e blocco locale attivato", True
    except Exception as exc:
        if type(exc).__name__ == "InviteRequestSentError":
            return "📨 Richiesta di accesso inviata — in attesa dell'approvazione degli amministratori", False
        if type(exc).__name__ == "UserAlreadyParticipantError":
            return "↪️ Telegram segnala account già membro", False
        return "❌ " + safe_connection_error(exc), False


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

        response = await user_client(GetParticipantRequest(destination, user))
        participant = getattr(response, "participant", None)
        if participant is None:
            return False, "InvalidParticipantResponse", "Risposta senza partecipante"
        rights = getattr(participant, "banned_rights", None)
        if (type(participant).__name__ == "ChannelParticipantLeft" or getattr(participant, "left", False)
                or getattr(rights, "view_messages", False)):
            return False, "UserNotParticipantError", "Partecipante uscito o escluso secondo Telegram"
        return True, None, None

    except Exception as e:
        if isinstance(e, FloodWaitError):
            await record_auto_deadline(e.seconds)
        return (
            False,
            type(e).__name__,
            str(e)[:220],
        )


def diagnostic_chat_id(destination):
    try:
        return utils.get_peer_id(destination)
    except (TypeError, ValueError):
        value = int(state["group_b"]["id"])
        return value if value < 0 else -1000000000000 - value


async def bot_membership_check(chat_id, user_id):
    if welcome_bot is None:
        return "unknown", "Bot non inizializzato", 0
    try:
        member = await asyncio.wait_for(welcome_bot.get_chat_member(chat_id, user_id), timeout=8)
        status = member.status
        if status in ("member", "administrator", "creator") or (status == "restricted" and member.is_member):
            return "present", str(status), 0
        if status in ("left", "kicked") or (status == "restricted" and not member.is_member):
            return "absent", str(status), 0
        return "unknown", "Stato Bot API non riconosciuto: " + str(status), 0
    except RetryAfter as exc:
        seconds = exc.retry_after.total_seconds() if hasattr(exc.retry_after, "total_seconds") else exc.retry_after
        return "pause", "RetryAfter", int(seconds)
    except Exception as exc:
        message = str(exc).lower()
        if "user_not_participant" in message or "user not participant" in message:
            return "absent", safe_connection_error(exc), 0
        return "unknown", safe_connection_error(exc), 0


async def observe_invite_membership(destination, user, mode, started_at):
    chat_id = diagnostic_chat_id(destination)
    began = asyncio.get_running_loop().time()
    ever_present = False
    final = "unknown"
    for offset in (10, 30, 60):
        await asyncio.sleep(max(0, offset - (asyncio.get_running_loop().time() - began)))
        telethon_result, bot_result = await asyncio.gather(
            asyncio.wait_for(verify_in_destination(destination, user), timeout=8),
            bot_membership_check(chat_id, user.id), return_exceptions=True)
        if isinstance(telethon_result, BaseException):
            present, error, detail = False, type(telethon_result).__name__, str(telethon_result)
        else:
            present, error, detail = telethon_result
        if isinstance(bot_result, BaseException):
            bot_status, bot_detail, pause = "unknown", type(bot_result).__name__, 0
        else:
            bot_status, bot_detail, pause = bot_result
        telethon_status = "present" if present else ("absent" if error == "UserNotParticipantError" else "unknown")
        await add_log(f"🔬 {mode} — ID {user.id} — gruppo B {chat_id} — controllo +{offset}s: sessione={telethon_status}; bot={bot_status} ({bot_detail[:100]})")
        if error in ("PeerFloodError", "FloodWaitError") or bot_status == "pause":
            if pause:
                await record_auto_deadline(pause)
            pause_detail = f"Bot API richiede una pausa di {pause} secondi" if bot_status == "pause" else detail
            return await report_verification_problem(str(user.id), mode, "FloodWaitError" if bot_status == "pause" else error, pause_detail)
        if error in DESTINATION_ERRORS:
            return await report_verification_problem(str(user.id), mode, error, detail)
        ever_present = ever_present or present or bot_status == "present"
        if telethon_status == "present" and bot_status != "absent":
            final = "confirmed"
        elif telethon_status == "absent" and bot_status == "absent":
            final = "absent"
        else:
            final = "unknown"
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT event, actor_id, event_date FROM membership_events WHERE chat_id=? AND user_id=? AND observed_at>=? ORDER BY id DESC LIMIT 3", (chat_id, user.id, started_at))
        events = await cursor.fetchall()
    for event, actor, date in reversed(events):
        await add_log(f"🧩 {mode} — ID {user.id} — gruppo {chat_id} — evento {event}; autore ID {actor or 'non disponibile'}; data Telegram {date or 'non disponibile'}")
    if final == "confirmed":
        await increment_stat("migrated")
        await save_processed(user, "CONFIRMED")
        await add_log(f"✅ {mode} — ID {user.id} — gruppo {chat_id} — PRESENZA CONFERMATA all'ultimo controllo")
        await welcome_confirmed_invite(destination, user)
        return "confirmed", str(user.id)
    await increment_stat("unconfirmed")
    if final == "absent":
        left_event = any(row[0] == "LEFT" for row in events)
        status = "LEFT_AFTER_JOIN" if ever_present or left_event else "NOT_ADDED"
        await save_processed(user, status)
        await add_log(f"⚠️ {mode} — ID {user.id} — gruppo {chat_id} — " +
                      ("USCITA/RIMOZIONE OSSERVATA" if left_event else "PRESENZA RILEVATA POI ASSENZA" if ever_present else "ASSENZA RILEVATA") +
                      "; causa non attribuita automaticamente; nessun reinvito durante la diagnosi", "WARNING")
        return "unconfirmed", str(user.id)
    # Discordanza o dati insufficienti non autorizzano un nuovo invito.
    await save_processed(user, "VERIFY_PENDING")
    return await report_verification_problem(str(user.id), mode, "MembershipUncertain", "Sessione e bot discordanti o stato non verificabile dopo 60 secondi; nessun reinvito")


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

async def remember_privacy_rejection(user_id, reason):
    owner = session_info[current_session_id()]["user_id"]
    if not owner or not state["group_b"]:
        return
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT OR REPLACE INTO privacy_rejections VALUES (?, ?, ?, ?, ?)",
                         (owner, user_id, state["group_b"]["id"], reason, now_it().isoformat()))
        await db.commit()


async def known_privacy_rejections():
    owner = session_info[current_session_id()]["user_id"]
    if not owner or not state["group_b"]:
        return set()
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT target_user_id FROM privacy_rejections WHERE account_user_id = ? AND destination_id = ?",
                                  (owner, state["group_b"]["id"]))
        return {row[0] for row in await cursor.fetchall()}


async def clean_member_rows(rows):
    """Rimuove solo presenze e rifiuti noti. Nessun invito di prova o invio esterno."""
    diag = {"already_b": 0, "privacy_known": 0, "optout": 0, "unknown": 0, "checked": 0, "note": ""}
    if not state["group_b"]:
        diag["unknown"] = len(rows)
        diag["note"] = "Gruppo B non configurato: pulizia non eseguita."
        return rows, diag
    destination = None
    wait_key = "member_check_wait_" + str(session_info[current_session_id()]["user_id"])
    wait = float(await get_setting(wait_key, "0")) - now_it().timestamp()
    if wait > 0:
        diag["note"] = f"Verifiche presenza sospese: pausa Telegram residua {math.ceil(wait)} secondi."
    else:
        try:
            destination = await asyncio.wait_for(user_client.get_entity(state["group_b"]["input"]), timeout=10)
        except Exception as exc:
            diag["note"] = "Gruppo B non verificabile — " + safe_connection_error(exc)
    known = await known_privacy_rejections()
    present, unknown = set(), set()
    deadline = asyncio.get_running_loop().time() + 30
    for row in rows:
        user_id, username = row[1], row[2]
        if destination is None or asyncio.get_running_loop().time() >= deadline:
            unknown.add(user_id)
            if destination is not None and not diag["note"]:
                diag["note"] = "Verifica presenza parziale: tempo massimo raggiunto. Usa PULISCI LISTA per aggiornarla."
            continue
        try:
            timeout = max(0.01, min(5, deadline - asyncio.get_running_loop().time()))
            entity = await asyncio.wait_for(user_client.get_input_entity(user_id), timeout=timeout)
            if getattr(entity, "user_id", None) != user_id:
                raise ValueError("Identità destinatario non verificata")
            timeout = max(0.01, min(5, deadline - asyncio.get_running_loop().time()))
            perm = await asyncio.wait_for(user_client.get_permissions(destination, entity), timeout=timeout)
            if perm is None:
                unknown.add(user_id)
            elif not perm.has_left and not getattr(getattr(getattr(perm, "participant", None), "banned_rights", None), "view_messages", False):
                present.add(user_id)
            diag["checked"] += 1
        except FloodWaitError as exc:
            await set_setting(wait_key, str(now_it().timestamp() + exc.seconds))
            diag["note"] = f"Telegram richiede {exc.seconds} secondi di pausa: verifiche interrotte."
            unknown.add(user_id)
            destination = None
        except PeerFloodError:
            for flag in ("telegram_locked", "telegram_restriction_detected"):
                await set_setting(flag, "1")
            sync_session_state()
            diag["note"] = "PeerFlood durante la verifica: blocco locale attivato e verifiche interrotte."
            unknown.add(user_id)
            destination = None
        except Exception as exc:
            if type(exc).__name__ == "UserNotParticipantError":
                diag["checked"] += 1
            else:
                unknown.add(user_id)
                if type(exc).__name__ in DESTINATION_ERRORS:
                    diag["note"] = "Verifiche interrotte — " + safe_connection_error(exc)
                    destination = None
    # Rispetta le rinunce anche se il dato sulla presenza non è disponibile.
    kept = []
    for row in rows:
        user_id = row[1]
        if user_id in present:
            diag["already_b"] += 1
        elif await invitation_opted_out(user_id):
            diag["optout"] += 1
        elif user_id in known:
            diag["privacy_known"] += 1
        else:
            kept.append(row)
            if user_id in unknown:
                diag["unknown"] += 1
    await add_log(f"🧹 PULIZIA LISTA — già B: {diag['already_b']} | privacy nota per questa sessione: {diag['privacy_known']} | rinunce: {diag['optout']} | presenza incerta conservata: {diag['unknown']}")
    return kept, diag


def cleanup_report(diag):
    return (f"🧹 Già nel gruppo B esclusi: {diag['already_b']}\n"
            f"🛡 Rifiuti privacy noti esclusi: {diag['privacy_known']}\n"
            f"🚪 Rinunce all'invito escluse: {diag['optout']}\n"
            f"❓ Presenza non verificabile, mantenuti: {diag['unknown']}\n"
            + (diag["note"] + "\n" if diag["note"] else "")
            + "La privacy degli altri utenti non è verificata. Nessun invito di prova.\n")


MEMBER_FILTER_DEFAULTS = {"username": True, "complete_name": False, "photo": False,
                          "no_admin": True, "seen_days": 0, "mode": "members",
                          "message_days": 7, "message_limit": 1000}


async def get_member_filters():
    try:
        saved = json.loads(await get_setting("member_filters", "{}"))
        if not isinstance(saved, dict):
            saved = {}
    except (TypeError, ValueError):
        saved = {}
    config = dict(MEMBER_FILTER_DEFAULTS)
    for key in ("username", "complete_name", "photo", "no_admin"):
        if isinstance(saved.get(key), bool):
            config[key] = saved[key]
    for key, options in (("seen_days", (0, 1, 7, 30)), ("message_days", (1, 7, 30)), ("message_limit", (500, 1000, 5000)), ("mode", ("members", "authors"))):
        if saved.get(key) in options:
            config[key] = saved[key]
    return config


def member_filter_reason(user, config, admin_ids, timestamp):
    if config["no_admin"] and user.id in admin_ids:
        return "admins"
    if config["username"] and not (getattr(user, "username", None) or "").strip():
        return "username"
    if config["complete_name"] and not all((getattr(user, field, None) or "").strip() for field in ("first_name", "last_name")):
        return "name"
    if config["photo"] and not getattr(getattr(user, "photo", None), "photo_id", None):
        return "photo"
    if config["seen_days"]:
        status = getattr(user, "status", None)
        if isinstance(status, UserStatusOnline):
            return None
        if not isinstance(status, UserStatusOffline) or not getattr(status, "was_online", None):
            return "status_unknown"
        age = timestamp - status.was_online.timestamp()
        if age < 0 or age > config["seen_days"] * 86400:
            return "old_status"
    return None


async def member_filters_text():
    config = await get_member_filters()
    return ("⚙️ FILTRI MEMBRI GRUPPO A\n\n"
            "I pulsanti configurano la prossima estrazione. La lista salvata cambia solo dopo ESTRAI/AGGIORNA.\n\n"
            + ("Fonte: lista membri visibile alla sessione." if config["mode"] == "members" else
               f"Fonte: autori dei messaggi negli ultimi {config['message_days']} giorni, massimo {config['message_limit']} messaggi esaminati.")
            + "\n\nNessun obbligo locale di iscrizione o ruolo amministratore: vengono letti i dati accessibili alla sessione secondo i permessi Telegram. Bot, account eliminati e duplicati sono esclusi. "
            "Le menzioni e i messaggi di servizio non vengono usati per selezionare utenti. "
            "Ultimo accesso nascosto/approssimativo: escluso se il filtro accesso è attivo. "
            "La lista non registra consenso a inviti o messaggi. Paese e nazionalità non sono dedotti dal profilo.")


async def member_filters_keyboard():
    config = await get_member_filters()
    rows = [[InlineKeyboardButton(("✅ " if config[key] else "❌ ") + label, callback_data="member_filter:" + key)]
            for key, label in (("username", "SOLO USERNAME"), ("complete_name", "NOME E COGNOME"), ("photo", "FOTO PROFILO"), ("no_admin", "ESCLUDI AMMINISTRATORI"))]
    rows.extend([
        [InlineKeyboardButton("ULTIMO ACCESSO: " + (f"{config['seen_days']} GIORNI" if config["seen_days"] else "NESSUN FILTRO"), callback_data="member_filter:seen_days")],
        [InlineKeyboardButton("FONTE: " + ("LISTA MEMBRI" if config["mode"] == "members" else "AUTORI MESSAGGI"), callback_data="member_filter:mode")],
        [InlineKeyboardButton(f"MESSAGGI: {config['message_days']} GIORNI", callback_data="member_filter:message_days"), InlineKeyboardButton(f"LIMITE: {config['message_limit']}", callback_data="member_filter:message_limit")],
        [InlineKeyboardButton("🔄 ESTRAI/AGGIORNA", callback_data="extract_members")],
        [InlineKeyboardButton("👥 LISTA SALVATA", callback_data="members_show")],
    ])
    return InlineKeyboardMarkup(rows)


async def extract_members_a():
    if not state["group_a"]:
        raise ValueError("Gruppo A non impostato")
    if operation_busy() or state["auto_enabled"]:
        raise ValueError("Disattiva AUTO e attendi la fine delle operazioni prima di estrarre.")
    source = await user_client.get_entity(state["group_a"]["input"])
    config = await get_member_filters()
    me = await user_client.get_me()
    stamp = now_it()
    diag = {key: 0 for key in ("telegram_count", "received", "bots", "deleted", "duplicates", "saved", "filtered", "status_unknown", "messages", "not_member")}
    diag["mode"] = config["mode"]
    try:
        participants = await user_client.get_participants(source, limit=1)
        diag["telegram_count"] = participants.total or 0
    except Exception:
        pass
    admin_ids = set()
    if config["no_admin"]:
        if isinstance(source, Channel):
            async for user in user_client.iter_participants(source, filter=ChannelParticipantsAdmins()):
                admin_ids.add(user.id)
        else:
            async for user in user_client.iter_participants(source):
                perm = await user_client.get_permissions(source, user)
                if perm.is_admin or perm.is_creator:
                    admin_ids.add(user.id)
    async def candidates():
        if config["mode"] == "members":
            async for user in user_client.iter_participants(source, aggressive=False):
                yield user
        else:
            cutoff = stamp - timedelta(days=config["message_days"])
            async for message in user_client.iter_messages(source, limit=config["message_limit"]):
                diag["messages"] += 1
                if message.date < cutoff:
                    break
                if getattr(message, "action", None) or getattr(message, "sender_id", None) is None:
                    continue
                user = await message.get_sender()
                if isinstance(user, User):
                    yield user
    rows, seen_ids = [], set()
    async for user in candidates():
        diag["received"] += 1
        if user.id == me.id:
            continue
        if getattr(user, "bot", False):
            diag["bots"] += 1
            continue
        if getattr(user, "deleted", False):
            diag["deleted"] += 1
            continue
        if user.id in seen_ids:
            diag["duplicates"] += 1
            continue
        seen_ids.add(user.id)
        reason = member_filter_reason(user, config, admin_ids, stamp.timestamp())
        if reason:
            diag["filtered"] += 1
            if reason == "status_unknown":
                diag["status_unknown"] += 1
            continue
        if config["mode"] == "authors":
            try:
                perm = await user_client.get_permissions(source, user)
                banned = getattr(getattr(perm, "participant", None), "banned_rights", None)
                if perm is None or perm.has_left or getattr(banned, "view_messages", False):
                    diag["not_member"] += 1
                    continue
            except Exception as exc:
                if type(exc).__name__ == "UserNotParticipantError":
                    diag["not_member"] += 1
                    continue
                raise
        display_name = " ".join(value for value in (getattr(user, "first_name", None), getattr(user, "last_name", None)) if value)
        rows.append((state["group_a"]["id"], user.id, getattr(user, "username", None), display_name, stamp.isoformat()))
    rows, diag["cleanup"] = await clean_member_rows(rows)
    # Non sostituisce la lista precedente in caso di errore o timeout della lettura.
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM extracted_members WHERE source_id = ?", (state["group_a"]["id"],))
        if rows:
            await db.executemany("INSERT OR REPLACE INTO extracted_members (source_id, user_id, username, display_name, extracted_at) VALUES (?, ?, ?, ?, ?)", rows)
        await db.commit()
    diag["saved"] = len(rows)
    state["extract_diag"] = diag
    await add_log(f"👥 ESTRAZIONE — fonte: {config['mode']} | ricevuti: {diag['received']} | esclusi filtri: {diag['filtered']} | salvati: {diag['saved']} — consenso non dedotto dalla lista")
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
        [InlineKeyboardButton("⚙️ FILTRI E FONTE", callback_data="member_filters")],
        [InlineKeyboardButton("🧹 PULISCI LISTA PER GRUPPO B", callback_data="clean_members")],
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
        rows.append((current_session_id(), user.id, username, display_name, phone, now_it().isoformat()))

    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("DELETE FROM session_contacts WHERE session_id = ?", (current_session_id(),))
        if rows:
            await db.executemany("""
                INSERT OR REPLACE INTO session_contacts
                (session_id, user_id, username, display_name, phone, extracted_at)
                VALUES (?, ?, ?, ?, ?, ?)
            """, rows)
        await db.commit()

    await add_log(f"📒 RUBRICA — contatti estratti: {len(rows)}")
    return len(rows)


async def contacts_count():
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("SELECT COUNT(*) FROM session_contacts WHERE session_id = ?", (current_session_id(),))
        row = await cur.fetchone()
    return int(row[0] if row else 0)


async def get_contacts_page(page):
    offset = page * PAGE_SIZE
    async with aiosqlite.connect(DB_PATH) as db:
        cur = await db.execute("""
            SELECT user_id, username, display_name
            FROM session_contacts
            WHERE session_id = ?
            ORDER BY CASE WHEN display_name IS NULL OR display_name = '' THEN 1 ELSE 0 END,
                     display_name COLLATE NOCASE,
                     username COLLATE NOCASE
            LIMIT ? OFFSET ?
        """, (current_session_id(), PAGE_SIZE, offset))
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
        [InlineKeyboardButton("👤 SELEZIONA SESSIONE", callback_data="sessions")],
        [InlineKeyboardButton("🌐 PROXY / DIAGNOSTICA", callback_data="proxy_status")],
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
        "👥 BESTPRICE MEMBER MANAGER V4.8.5\n\n"
        f"👤 SESSIONE ATTIVA: {session_label()}\n"
        f"🔌 {'Connessa' if session_info[current_session_id()]['ready'] else 'Non disponibile'}\n\n"
        f"📥 GRUPPO A: {group_label(state['group_a'])}\n"
        f"📤 GRUPPO B: {group_label(state['group_b'])}\n\n"
        f"{status}"
    )


# =========================================================
# INVITO SINGOLO
# =========================================================

DESTINATION_ERRORS = {
    "ChatWriteForbiddenError", "ChatAdminRequiredError", "ChannelPrivateError",
    "ChannelInvalidError", "ChatInvalidError", "UserBannedInChannelError",
    "UsersTooMuchError",
}
STOP_INVITE_RESULTS = {"peer_flood", "flood_wait", "destination_error", "verification_error", "cooldown_stopped"}


async def stop_invites_for_destination(display, mode, error_name, message):
    state["running"] = False
    state["auto_enabled"] = False
    await set_setting("auto_enabled", "0")
    await increment_stat("errors")
    await add_log(
        f"🛑 {mode} — {display} — PROBLEMA GRUPPO B — "
        f"{error_name}: {message[:220]} — ciclo e programmazione AUTO fermati. "
        "Controlla accesso, permessi e capacità del gruppo B per questa sessione. "
        "Nessun blocco antispam locale impostato.", "ERROR",
    )
    return ("destination_error", display)


async def report_verification_problem(display, mode, error_name, message):
    if error_name == "PeerFloodError":
        state["running"] = False
        state["auto_enabled"] = False
        state["telegram_locked"] = True
        state["telegram_restriction_detected"] = True
        await set_setting("auto_enabled", "0")
        await set_setting("telegram_locked", "1")
        await set_setting("telegram_restriction_detected", "1")
        await add_log(
            f"🛑 {mode} — {display} — LIMITAZIONE DURANTE LA VERIFICA — "
            f"{error_name}: {(message or '')[:220]} — ciclo fermato e blocco locale attivato", "WARNING",
        )
        return ("peer_flood", display)
    if error_name == "FloodWaitError":
        state["running"] = False
        state["auto_enabled"] = False
        await set_setting("auto_enabled", "0")
        await add_log(
            f"⏳ {mode} — {display} — ATTESA TELEGRAM DURANTE LA VERIFICA — "
            f"{error_name}: {(message or '')[:220]} — ciclo fermato; rispetta l'attesa richiesta", "WARNING",
        )
        return ("flood_wait", display)
    if error_name in DESTINATION_ERRORS:
        return await stop_invites_for_destination(display, mode, error_name, message or "")
    state["running"] = False
    state["auto_enabled"] = False
    await set_setting("auto_enabled", "0")
    await increment_stat("errors")
    await add_log(
        f"⚠️ {mode} — {display} — PRESENZA NON VERIFICABILE — "
        f"{error_name or 'ErroreSconosciuto'}: {(message or '')[:220]} — "
        "ciclo e programmazione AUTO fermati; nessun nuovo invito. "
        "L'esito resta da verificare.", "WARNING",
    )
    return ("verification_error", display)


def missing_invitee_detail(item):
    """Legge solo i campi documentati, senza esporre l'intera risposta."""
    premium_invite = getattr(item, "premium_would_allow_invite", None)
    premium_pm = getattr(item, "premium_required_for_pm", None)
    if premium_invite:
        reason = "Telegram indica che l'account invitante necessita di Premium per questo invito"
    elif premium_pm:
        reason = "Telegram indica privacy del destinatario e requisito Premium per un messaggio privato"
    elif hasattr(item, "premium_would_allow_invite") and hasattr(item, "premium_required_for_pm"):
        reason = "Telegram indica che le impostazioni privacy impediscono l'aggiunta diretta"
    else:
        reason = "Telegram segnala il destinatario come non invitato; causa non specificata"
    return (f"{reason}; premium_would_allow_invite={premium_invite}; "
            f"premium_required_for_pm={premium_pm}")


def auto_deadline_key():
    owner = session_info[current_session_id()].get("user_id")
    return f"auto_next_invite_{owner or ('slot_' + str(current_session_id()))}"


async def record_auto_deadline(seconds):
    key = auto_deadline_key()
    existing = float(await get_setting(key, "0"))
    deadline = max(existing, now_it().timestamp() + seconds)
    await set_setting(key, str(deadline))


async def wait_auto_deadline():
    deadline = float(await get_setting(auto_deadline_key(), "0"))
    remaining = max(0, deadline - now_it().timestamp())
    if not remaining:
        return True
    minutes = math.ceil(remaining / 60)
    await add_log(f"⏱ AUTO — pausa precedente ancora attiva: attesa fino a {minutes} minuti prima di un nuovo invito")
    return await interruptible_wait(minutes)


async def invite_one(destination, user, mode):
    if await invitation_opted_out(user.id):
        await add_log(f"🚪 {mode} — ID {user.id} — uscita richiesta dall'utente: invito escluso")
        return ("privacy", str(user.id))
    if user.id in await known_privacy_rejections():
        await add_log(f"🛡 {mode} — ID {user.id} — rifiuto privacy già registrato per questo account e gruppo; nessun nuovo invito")
        return ("privacy", str(user.id))
    display = f"@{user.username}" if getattr(user, "username", None) else f"ID {user.id}"
    previous_status = await processed_status(user.id)
    pending = previous_status in {"UNCONFIRMED", "VERIFY_PENDING"}
    recovery = pending or previous_status == "NOT_ADDED"
    await add_log(
        f"🔍 {mode} — {display} — "
        + ("Recupero esito precedente: verifica presenza prima di un eventuale unico nuovo tentativo" if recovery
           else "Controllo presenza nel gruppo B prima dell'invito")
    )
    confirmed, error_name, error_message = await verify_in_destination(destination, user)
    if confirmed:
        if pending:
            await increment_stat("migrated")
            await save_processed(user, "CONFIRMED")
            await add_log(f"✅ {mode} — {display} — PRESENZA CONFERMATA dopo esito precedente incerto")
            await welcome_confirmed_invite(destination, user)
            return ("confirmed", display)
        await increment_stat("already")
        await save_processed(user, "ALREADY")
        await add_log(f"↪️ {mode} — {display} — GIÀ PRESENTE nel gruppo B; nessun invito")
        return ("already", display)
    if error_name != "UserNotParticipantError":
        return await report_verification_problem(display, mode, error_name, error_message)
    if previous_status in {"PRIVACY", "INVITE_REJECTED"}:
        await add_log(f"🔒 {mode} — {display} — precedente rifiuto privacy/invito: nessun nuovo tentativo", "WARNING")
        return ("unconfirmed", display)
    if recovery and await recovery_used(user.id):
        await save_processed(user, "NOT_ADDED")
        await add_log(f"↪️ {mode} — {display} — recupero già usato; nessun nuovo invito")
        return ("unconfirmed", display)
    if "AUTO" in mode and not await wait_auto_deadline():
        return ("cooldown_stopped", display)
    if recovery:
        # L'assenza è certa (UserNotParticipantError), non un errore tecnico.
        if not await reserve_recovery(user.id):
            await save_processed(user, "NOT_ADDED")
            await add_log(
                f"↪️ {mode} — {display} — assenza verificata; unico tentativo di recupero già usato. "
                "Nessun nuovo invito e nessuna attesa di invio", "WARNING",
            )
            return ("unconfirmed", display)
        await add_log(
            f"🔄 {mode} — {display} — ASSENZA VERIFICATA — nuovo tentativo di recupero 1/1 con questa sessione"
        )


    if "AUTO" in mode:
        await record_auto_deadline(max(15, state["interval_minutes"]) * 60)
    await increment_stat("attempts")
    started_at = now_it().isoformat()
    await add_log(f"{mode} — Tentativo: {display} — ID {user.id} — gruppo B {diagnostic_chat_id(destination)}")
    try:
        response = await user_client(InviteToChannelRequest(destination, [user]))
        # Persistenza immediata: una verifica inconcludente non deve causare reinviti.
        await save_processed(user, "VERIFY_PENDING")
        missing = getattr(response, "missing_invitees", None)
        matched = [item for item in (missing or []) if getattr(item, "user_id", None) == user.id]
        await add_log(
            f"📨 {mode} — {display} — RISPOSTA TELEGRAM: {type(response).__name__}; "
            + (f"missing_invitees={len(missing)}" if missing is not None
               else "missing_invitees non disponibile in questa risposta")
            + f" — ID {user.id}; gruppo B {diagnostic_chat_id(destination)} — risposta ricevuta; aggiunta non ancora confermata"
        )
        if matched:
            for item in matched:
                if (hasattr(item, "premium_would_allow_invite") and hasattr(item, "premium_required_for_pm")
                        and not getattr(item, "premium_would_allow_invite", False)):
                    await remember_privacy_rejection(user.id, "missing_invitees: privacy")
            detail = " | ".join(missing_invitee_detail(item) for item in matched)
            await increment_stat("unconfirmed")
            await save_processed(user, "INVITE_REJECTED")
            await add_log(
                f"🚫 {mode} — {display} (ID {user.id}) — NON INVITATO — "
                f"missing_invitees: {detail}", "WARNING",
            )
            return ("unconfirmed", display)

        await add_log(f"🔍 {mode} — ID {user.id} — gruppo {diagnostic_chat_id(destination)} — controlli presenza a 10, 30 e 60 secondi; nessun nuovo invito")
        return await observe_invite_membership(destination, user, mode, started_at)

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
        await remember_privacy_rejection(user.id, type(e).__name__)

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
        await remember_privacy_rejection(user.id, type(e).__name__)

        await add_log(
            f"🔒 {mode} — {display} — PRIVACY/CONTATTO — "
            "Telegram richiede che l'utente sia un contatto reciproco — "
            f"{type(e).__name__}: {str(e)[:220]}",
            "WARNING",
        )

        return ("privacy", display)

    except FloodWaitError as e:

        state["running"] = False
        state["auto_enabled"] = False
        await set_setting("auto_enabled", "0")
        await record_auto_deadline(e.seconds)

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

        if isinstance(e, (TimeoutError, ConnectionError, OSError)):
            # La richiesta potrebbe essere arrivata a Telegram prima della caduta
            # di connessione: il tentativo seguente dovrà soltanto verificarla.
            await save_processed(user, "VERIFY_PENDING")
            return await report_verification_problem(display, mode, error_name, str(e))

        if error_name in DESTINATION_ERRORS:
            return await stop_invites_for_destination(display, mode, error_name, str(e))

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
            if not control_lock.locked():
                async with control_lock:
                    if (state["auto_enabled"] and not operation_busy()
                            and not state["telegram_locked"]
                            and session_info[state["active_session"]]["ready"]
                            and state["group_a"] and state["group_b"]):
                        now = now_it()
                        today = now.date().isoformat()
                        stats = await get_today_stats()
                        if (now.strftime("%H:%M") >= state["start_time"]
                                and state["last_autostart_day"] != today
                                and stats["migrated"] < state["daily_target"]):
                            state["last_autostart_day"] = today
                            await set_setting("last_autostart_day", today)
                            state["running"] = True
                            state["stop_requested"] = False
                            await add_log(f"⏰ Partenza automatica programmata delle {state['start_time']}")
                            worker_task = asyncio.create_task(migration_worker(application))
            await asyncio.sleep(15)
        except asyncio.CancelledError:
            break
        except Exception as exc:
            logger.exception("Errore daily_scheduler")
            await add_log(f"❌ Scheduler — {type(exc).__name__}: {str(exc)[:180]}", "ERROR")
            await asyncio.sleep(30)


# =========================================================
# WORKER AUTOMATICO
# =========================================================

@pinned_session
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

                attempts_before = stats["attempts"]

                result, display = (
                    await invite_one(
                        destination,
                        user,
                        "🤖 AUTO",
                    )
                )

                if result in STOP_INVITE_RESULTS:
                    break

                stats = await get_today_stats()
                sent_attempt = stats["attempts"] > attempts_before
                status = await processed_status(user.id)
                definite_skip = result in {"privacy", "already"} or status in {
                    "PRIVACY", "INVITE_REJECTED", "ALREADY", "RECIPIENT_ERROR", "CHANNEL_LIMIT"
                }
                if sent_attempt and not definite_skip:
                    attempts_cycle += 1

                if (
                    stats["migrated"]
                    >= state["daily_target"]
                ):

                    await add_log(
                        "🎯 Obiettivo "
                        "giornaliero raggiunto"
                    )

                    break

                if attempts_cycle >= state["max_attempts"]:
                    await add_log("🛑 MAX tentativi raggiunto")
                    break
                if not sent_attempt:
                    await add_log(
                        f"↪️ 🤖 AUTO — {display} — solo controllo, nessun invito inviato; passo al prossimo utente"
                    )
                    continue

                if state["running"]:

                    await add_log(
                        "⏱ Richiesta d'invito effettuata: prossimo tentativo "
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
                    "SELECT username FROM session_contacts WHERE session_id = ? AND user_id = ? LIMIT 1",
                    (current_session_id(), int(user_id)),
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
            "destination_error": "🛑",
            "verification_error": "⚠️",
        }

        results.append(
            f"{labels.get(result, '❓')} {display}"
        )

        if result in STOP_INVITE_RESULTS:
            break

    state["manual_ids"] = []
    state["manual_refs"] = []

    return results


@pinned_session
async def run_contact_queue(bot, chat_id, refs):
    """Aggiunge il blocco Rubrica in background rispettando il timing dedicato."""
    try:
        destination = await user_client.get_entity(state["group_b"]["input"])
        total = len(refs)
        interrupted = False
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
                if result in STOP_INVITE_RESULTS:
                    interrupted = True
                    break
            if index < total and not state["telegram_locked"]:
                minutes = state["contact_interval_minutes"]
                await add_log(f"⏱ 📒 RUBRICA — prossimo contatto tra {minutes} minuti")
                await asyncio.sleep(minutes * 60)
        if state["telegram_locked"]:
            await bot.send_message(chat_id, "🔒 Coda Rubrica interrotta per una limitazione Telegram. Controlla il LOG.")
        elif interrupted:
            await bot.send_message(chat_id, "🛑 Coda Rubrica interrotta. Controlla il LOG per il motivo; i contatti successivi non sono stati tentati.")
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

@serialized_control
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

async def callback_notice(query, text, show_alert=False):
    """L'ack iniziale chiude lo spinner; gli avvisi successivi restano in chat."""
    rows = []
    if state["running"] or (worker_task is not None and not worker_task.done()):
        rows.append([InlineKeyboardButton("🛑 FERMA AUTO", callback_data="stop")])
    rows.append([InlineKeyboardButton("⬅️ HOME", callback_data="home")])
    await query.message.reply_text(text, reply_markup=InlineKeyboardMarkup(rows))
    await add_log(f"ℹ️ Pannello — {text}", "WARNING")


@serialized_control
async def buttons(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
):

    global worker_task, contact_task

    if not is_admin(update):

        await deny_access(update)
        return

    query = update.callback_query

    await query.answer()

    data = query.data
    if data.startswith(("profile_new:", "profile_regen:", "profile_apply:")) or data == "profile_cancel":
        await profile_action(update, context)
        return
    if data == "home":
        context.user_data.pop("profile_pending", None)
        context.user_data.pop("join_a_pending", None)

    if data == "clean_members":
        if operation_busy() or state["auto_enabled"]:
            await callback_notice(query, "Disattiva AUTO e attendi la fine delle operazioni.", show_alert=True)
            return
        if not state["group_a"] or not state["group_b"]:
            await callback_notice(query, "Configura prima i gruppi A e B.", show_alert=True)
            return
        await query.edit_message_text("🧹 Controllo presenze nel gruppo B e rifiuti privacy già registrati… Nessun invito verrà inviato.")
        try:
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute("SELECT source_id, user_id, username, display_name, extracted_at FROM extracted_members WHERE source_id = ?", (state["group_a"]["id"],))
                rows = await cursor.fetchall()
            kept, diag = await clean_member_rows(rows)
            kept_ids = {row[1] for row in kept}
            async with aiosqlite.connect(DB_PATH) as db:
                await db.executemany("DELETE FROM extracted_members WHERE source_id = ? AND user_id = ?",
                                     [(row[0], row[1]) for row in rows if row[1] not in kept_ids])
                await db.commit()
            state["member_page"] = 0
            await query.edit_message_text("🧹 PULIZIA COMPLETATA\n\n" + cleanup_report(diag) + f"\nRimasti nella lista: {len(kept)}", reply_markup=members_keyboard())
        except Exception as exc:
            await query.edit_message_text("❌ Pulizia non completata — " + safe_connection_error(exc), reply_markup=members_keyboard())
        return
    if data == "member_filters" or data.startswith("member_filter:"):
        if operation_busy() or state["auto_enabled"]:
            await callback_notice(query, "Disattiva AUTO e attendi la fine delle operazioni.", show_alert=True)
            return
        if data.startswith("member_filter:"):
            key = data.split(":", 1)[1]
            config = await get_member_filters()
            if key in ("username", "complete_name", "photo", "no_admin"):
                config[key] = not config[key]
            elif key in ("seen_days", "message_days", "message_limit", "mode"):
                options = {"seen_days": (0, 1, 7, 30), "message_days": (1, 7, 30), "message_limit": (500, 1000, 5000), "mode": ("members", "authors")}[key]
                config[key] = options[(options.index(config[key]) + 1) % len(options)]
            else:
                await callback_notice(query, "Filtro non valido.")
                return
            await set_setting("member_filters", json.dumps(config))
        await query.edit_message_text(await member_filters_text(), reply_markup=await member_filters_keyboard())
        return
    if data == "join_a_setup":
        if operation_busy() or state["auto_enabled"]:
            await callback_notice(query, "Disattiva AUTO e attendi la fine delle operazioni prima di procedere.", show_alert=True)
            return
        if not state["group_a"]:
            await callback_notice(query, "Configura prima il gruppo A.", show_alert=True)
            return
        state["waiting_for"] = "join_a_link"
        context.user_data.pop("join_a_pending", None)
        await query.edit_message_text("📥 INGRESSO DELLE SEI SESSIONI NEL GRUPPO A\n\n"
            + state["group_a"]["name"] + "\nIncolla il link d'invito del gruppo A oppure il suo @username. "
            "Ti mostrerò una conferma prima di procedere.", reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("❌ ANNULLA", callback_data="home")]]))
        return
    if data == "join_a_confirm":
        pending = context.user_data.get("join_a_pending")
        if operation_busy() or state["auto_enabled"]:
            await callback_notice(query, "Disattiva AUTO e attendi la fine delle operazioni.", show_alert=True)
            return
        if not pending or not state["group_a"] or pending["group"] != state["group_a"] or now_it().timestamp() - pending["created_at"] > 600:
            context.user_data.pop("join_a_pending", None)
            await callback_notice(query, "Conferma scaduta: riapri il pulsante di ingresso.", show_alert=True)
            return
        context.user_data.pop("join_a_pending", None)
        state["waiting_for"] = None
        lines = ["📥 INGRESSO SESSIONI — " + pending["group"]["name"]]
        for index, account_id in enumerate(ACCOUNT_IDS):
            await query.edit_message_text("\n".join(lines) + f"\n⏳ Controllo ACCOUNT {account_id}…")
            outcome, stop = await join_source_account(account_id, pending["reference"], pending["group"])
            lines.append(f"ACCOUNT {account_id}: {outcome}")
            await add_log("📥 INGRESSO GRUPPO A — " + outcome, session_id=account_id)
            if stop:
                lines.append("Operazione fermata. Gli account successivi non sono stati tentati.")
                break
            if index < len(ACCOUNT_IDS) - 1:
                await asyncio.sleep(2)
        await query.edit_message_text("\n".join(lines), reply_markup=sessions_keyboard())
        return
    if data == "proxy_status":
        await query.edit_message_text(await proxy_status_text(), reply_markup=proxy_keyboard())
        return
    if data.startswith("proxy_test:") or data == "proxy_ip":
        if operation_busy():
            await callback_notice(query, "Ferma le operazioni prima di eseguire la diagnostica proxy.")
            return
        await query.edit_message_text("🌐 Verifica proxy in corso… Nessun invito verrà inviato.")
        if data == "proxy_ip":
            await check_telegram_ip(current_session_id())
        else:
            value = data.split(":", 1)[1]
            if value not in {"1", "2"}:
                await callback_notice(query, "Proxy non valido.")
                return
            await test_proxy_connection(int(value))
        await query.edit_message_text(await proxy_status_text(), reply_markup=proxy_keyboard())
        return
    if data == "sessions":
        await query.edit_message_text(await sessions_text(), reply_markup=sessions_keyboard())
        return
    if data.startswith("select_session:"):
        account_id = int(data.split(":", 1)[1])
        try:
            await select_session(account_id)
        except ValueError as exc:
            await callback_notice(query, str(exc), show_alert=True)
            return
        await query.edit_message_text(await home_text(), reply_markup=main_keyboard())
        return
    if data.startswith("check_session:"):
        account_id = int(data.split(":", 1)[1])
        if operation_busy():
            await callback_notice(query, "Ferma le operazioni prima di verificare la connessione.", show_alert=True)
            return
        if account_id not in ACCOUNT_IDS:
            await callback_notice(query, "Sessione non valida.")
            return
        await query.edit_message_text(f"🔎 Verifica dettagliata ACCOUNT {account_id} in corso… Contatto @SpamBot con /start; nessun invito verrà inviato.")
        await connect_session(account_id)
        await add_log("🔎 Connessione verificata: " + session_info[account_id]["error"],
                      session_id=account_id)
        await query_spambot(account_id)
        report = await session_diagnostic_text(account_id)
        # Mantiene tutte le informazioni anche quando il report supera il limite Telegram.
        chunks = []
        current = ""
        for line in report.splitlines():
            if len(current) + len(line) + 1 > 3800:
                chunks.append(current)
                current = ""
            current += line + "\n"
        if current:
            chunks.append(current)
        await query.edit_message_text(chunks[0], reply_markup=diagnostic_keyboard())
        for chunk in chunks[1:]:
            await query.message.reply_text(chunk, reply_markup=diagnostic_keyboard())
        return
    if data.startswith("unlock:"):
        if data != f"unlock:{state['active_session']}":
            await callback_notice(query, "La sessione è cambiata: riapri lo sblocco dalla Home.", show_alert=True)
            return
        data = "unlock"
    elif data == "unlock":
        await callback_notice(query, "Riapri lo sblocco dalla Home per confermare la sessione.", show_alert=True)
        return
    if data == "unlock":
        if operation_busy():
            await callback_notice(query, "Attendi la fine delle operazioni prima dello sblocco.", show_alert=True)
            return
    if data in {"confirm_run", "start_now", "manual_confirm", "contacts_confirm"}:
        if not session_info[current_session_id()]["ready"]:
            await callback_notice(query, "Sessione non disponibile. Apri SELEZIONA SESSIONE.", show_alert=True)
            return
        if operation_busy():
            await callback_notice(query, "Un'operazione è già in corso: attendi la fine o ferma il ciclo AUTO.", show_alert=True)
            return
        if data in {"manual_confirm", "contacts_confirm", "confirm_run"}:
            if state.get("prepared_session") != current_session_id():
                await callback_notice(query, "Conferma scaduta: prepara di nuovo l'operazione con la sessione attiva.", show_alert=True)
                return
    if data in {"set_a", "set_b"} and operation_busy():
        await callback_notice(query, "Non puoi cambiare gruppi durante un'operazione.", show_alert=True)
        return

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
            > 15
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
        state["prepared_session"] = current_session_id()

        if state["telegram_locked"]:

            await callback_notice(query, 
                "🔒 Inviti sospesi.",
                show_alert=True,
            )

            return

        if state["running"]:

            await callback_notice(query, 
                "AUTO già attivo.",
                show_alert=True,
            )

            return

        if (
            not state["group_a"]
            or not state["group_b"]
        ):

            await callback_notice(query, 
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
            await callback_notice(query, 
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
            await callback_notice(query, 
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
            await callback_notice(query, "🔒 Inviti sospesi.", show_alert=True)
            return

        if state["running"] or (worker_task is not None and not worker_task.done()):
            await callback_notice(query, 
                "🟢 Automatico già in esecuzione. Non è stato avviato un secondo ciclo.",
                show_alert=True,
            )
            return

        if not state["group_a"] or not state["group_b"]:
            await callback_notice(query, "Imposta prima A e B.", show_alert=True)
            return

        stats = await get_today_stats()
        if stats["migrated"] >= state["daily_target"]:
            await callback_notice(query, 
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

            await callback_notice(query, 
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
                await asyncio.wait_for(extract_members_a(), timeout=120)
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

                f"⚙️ Esclusi dai filtri: {diag['filtered']}\n"
                f"❓ Accesso non verificabile escluso: {diag['status_unknown']}\n"
                f"📝 Messaggi esaminati: {diag['messages']}\n"
                f"🚪 Autori non più membri: {diag['not_member']}\n"
                f"📂 Fonte: {diag['mode']}\n"
                f"💾 Salvati: "
                f"{diag['saved']}\n\n"

                + cleanup_report(diag["cleanup"]) + "\n"
                + f"📋 Disponibili nella lista: "
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
                f"{safe_connection_error(e)}\n\n"
                "La lettura non completata non sostituisce la lista precedente. Controlla il LOG.",
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
            await callback_notice(query, "Imposta prima il gruppo B.", show_alert=True)
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
            await callback_notice(query, "🔒 Inviti sospesi.", show_alert=True)
            return
        if state.get("contact_queue_running"):
            await callback_notice(query, "📒 Una coda Rubrica è già in esecuzione.", show_alert=True)
            return
        refs = list(state.get("manual_refs") or [])
        if not refs:
            await callback_notice(query, "Nessun contatto preparato.", show_alert=True)
            return
        state["contact_queue_running"] = True
        contact_task = asyncio.create_task(run_contact_queue(context.bot, query.message.chat_id, refs))
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

            await callback_notice(query, 
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

            await callback_notice(query, 
                "🔒 Inviti sospesi.",
                show_alert=True,
            )

            return

        await query.edit_message_text(
            "⏳ Elaborazione manuale..."
        )

        state["manual_running"] = True
        try:
            results = await run_manual_invites()
        finally:
            state["manual_running"] = False

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

    elif data == "logs" or data.startswith("logs:"):
        selected_filter = data.split(":", 1)[1] if ":" in data else "all"
        if selected_filter not in {"all", *(str(i) for i in ACCOUNT_IDS)}:
            return
        async with aiosqlite.connect(DB_PATH) as db:
            if selected_filter == "all":
                cursor = await db.execute("SELECT created_at, message FROM logs ORDER BY id DESC LIMIT 20")
            else:
                cursor = await db.execute(
                    "SELECT created_at, message FROM logs WHERE session_id = ? ORDER BY id DESC LIMIT 20",
                    (int(selected_filter),),
                )
            rows = await cursor.fetchall()
        lines = []
        budget = 0
        for created_at, message in rows:
            try:
                dt = datetime.fromisoformat(created_at)
                dt = dt.replace(tzinfo=ITALY_TZ) if dt.tzinfo is None else dt.astimezone(ITALY_TZ)
                stamp = dt.strftime("%H:%M:%S")
            except (ValueError, TypeError):
                stamp = "--:--:--"
            line = f"{stamp}  {message}"
            if budget + len(line) + 1 > 3700:
                break
            lines.append(line)
            budget += len(line) + 1
        title = "TUTTE LE SESSIONI" if selected_filter == "all" else f"ACCOUNT {selected_filter}"
        await query.edit_message_text(
            f"📋 ULTIMI EVENTI — {title}\n\n" + ("\n".join(reversed(lines)) or "Nessun evento."),
            reply_markup=InlineKeyboardMarkup([
                [InlineKeyboardButton("TUTTE", callback_data="logs:all")],
                *[[InlineKeyboardButton(f"ACCOUNT {i}", callback_data=f"logs:{i}")
                   for i in ACCOUNT_IDS[start:start + 3]] for start in (0, 3)],
                [InlineKeyboardButton("🔄 AGGIORNA", callback_data=f"logs:{selected_filter}")],
                [InlineKeyboardButton("🗑 PULISCI TUTTI I LOG", callback_data="clear_logs_confirm")],
                [InlineKeyboardButton("⬅️ INDIETRO", callback_data="home")],
            ]),
        )

    elif data == "clear_logs_confirm":

        await query.edit_message_text(
            "⚠️ Cancellare tutti i log, di tutte le sei sessioni e quelli precedenti?",
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
            f"⚠️ RIABILITARE GLI INVITI?\n{session_label()}\n\n"
            "Il blocco locale è stato attivato perché Telegram ha segnalato una limitazione.\n\n"
            "Sblocca solo dopo aver verificato (ad esempio con @SpamBot) "
            "che l'account non sia più limitato. Lo sblocco del bot non rimuove "
            "una limitazione applicata da Telegram.",

            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton(
                    "🔓 CONFERMA",
                    callback_data=f"unlock:{state['active_session']}",
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
            "su conferma dell’amministratore; nessuna verifica automatica della limitazione Telegram"
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

@serialized_control
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

    if target == "join_a_link":
        if operation_busy() or state["auto_enabled"] or not state["group_a"]:
            await update.message.reply_text("Disattiva AUTO e configura il gruppo A prima di procedere.")
            return
        try:
            reference = parse_join_reference(update.message.text)
            ready = next((account_id for account_id in ACCOUNT_IDS if session_info[account_id]["ready"]), None)
            if ready is None:
                raise ValueError("Nessuna sessione connessa: usa VERIFICA e riprova.")
            _, title, id_known = await inspect_join_target(session_clients[ready], reference, state["group_a"])
            context.user_data["join_a_pending"] = {"reference": reference, "group": dict(state["group_a"]), "created_at": now_it().timestamp()}
            state["waiting_for"] = None
            await update.message.reply_text("📥 CONFERMA INGRESSO DELLE SEI SESSIONI\n\n"
                + title + "\nLink/username: " + reference["display"] + "\n\n"
                + ("Identità del gruppo verificata." if id_known else "Il link privato espone solo il nome, non l'ID. Conferma che questo sia il link del gruppo A.")
                + "\nGli account già membri saranno saltati. Le richieste soggette ad approvazione resteranno in attesa. I blocchi sugli inviti rimangono invariati.",
                reply_markup=InlineKeyboardMarkup([[InlineKeyboardButton("✅ CONFERMA INGRESSO", callback_data="join_a_confirm")], [InlineKeyboardButton("❌ ANNULLA", callback_data="home")]]))
        except Exception as exc:
            context.user_data.pop("join_a_pending", None)
            await update.message.reply_text("❌ " + safe_connection_error(exc) + "\nIncolla un link valido del gruppo A.")
        return

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
        state["prepared_session"] = current_session_id()
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

    if operation_busy():
        await update.message.reply_text("Attendi la fine delle operazioni prima di cambiare gruppo.")
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

    global welcome_bot
    welcome_bot = application.bot
    await init_db()
    await load_settings()
    await ensure_today()

    for account_id in ACCOUNT_IDS:
        await connect_session(account_id)
        await add_log("🔌 Connessione iniziale — " + session_info[account_id]["error"], session_id=account_id)
    available = [account_id for account_id in ACCOUNT_IDS if session_info[account_id]["ready"]]
    if not available:
        logger.warning("Nessuna sessione disponibile: il pannello resta accessibile per la diagnostica")
        state["auto_enabled"] = False
        await set_setting("auto_enabled", "0")
    elif state["active_session"] not in available:
        state["active_session"] = available[0]
        sync_session_state()
        await set_setting("active_session", state["active_session"])
        state["auto_enabled"] = False
        await set_setting("auto_enabled", "0")
    await add_log("⚙️ Avvio V4.8.5 — " + session_info[state["active_session"]]["error"])

    global scheduler_task
    scheduler_task = asyncio.create_task(
        daily_scheduler(application)
    )


async def post_shutdown(
    application,
):

    global scheduler_task, worker_task, contact_task
    state["running"] = False
    state["stop_requested"] = True
    tasks = [task for task in (scheduler_task, worker_task, contact_task) if task is not None]
    tasks.extend(list(welcome_tasks))
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)
    for client in session_clients.values():
        await client.disconnect()


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

    # Il prefisso -100 è una marcatura Bot API, non parte dell'ID positivo.
    def unmarked(value):
        if value <= -1000000000000:
            return -value - 1000000000000
        return abs(value)
    return unmarked(a) == unmarked(b)


async def _delete_welcome_later(bot, chat_id, message_id):
    await asyncio.sleep(WELCOME_DELETE_SECONDS)
    try:
        await bot.delete_message(chat_id=chat_id, message_id=message_id)
    except Exception as e:
        logger.warning("Impossibile eliminare il benvenuto %s: %s", message_id, e)


async def record_membership_event(message, chat, member, event):
    actor = getattr(getattr(message, "from_user", None), "id", None)
    date = getattr(message, "date", None)
    async with aiosqlite.connect(DB_PATH) as db:
        await db.execute("INSERT INTO membership_events(chat_id,user_id,event,actor_id,event_date,observed_at) VALUES (?,?,?,?,?,?)",
                         (chat.id, member.id, event, actor, date.isoformat() if date else None, now_it().isoformat()))
        await db.commit()
    await add_log(f"📥 EVENTO {event} — ID membro {member.id} — gruppo {chat.id} — autore ID {actor or 'non disponibile'} — data Telegram {date.isoformat() if date else 'non disponibile'} — messaggio {message.message_id}", session_id=0)


async def delete_left_member_notice(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Registra l'uscita e lascia visibile il messaggio durante la diagnosi."""
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

    await record_membership_event(message, chat, message.left_chat_member, "LEFT")
    async with welcome_lock:
        async with aiosqlite.connect(DB_PATH) as db:
            await db.execute("DELETE FROM welcome_sent WHERE chat_id = ? AND user_id = ?", (chat.id, message.left_chat_member.id))
            await db.commit()
    # Diagnosi: lascia visibile l'avviso d'uscita. Non rimuove nessun membro.
    await add_log("🔎 Avviso di uscita mantenuto visibile per la diagnosi", session_id=0)


async def send_welcome_once(bot, chat_id, member, origin, session_id=0):
    """Unica funzione per evento d'ingresso e fallback dopo invito confermato."""
    if getattr(member, "is_bot", False) or getattr(member, "bot", False):
        return False
    user_id = int(member.id)
    try:
        async with welcome_lock:
            async with aiosqlite.connect(DB_PATH) as db:
                cursor = await db.execute(
                    "SELECT sent_at FROM welcome_sent WHERE chat_id = ? AND user_id = ?",
                    (chat_id, user_id),
                )
                row = await cursor.fetchone()
            if row:
                stamp = datetime.fromisoformat(row[0])
                if stamp.tzinfo is None:
                    stamp = stamp.replace(tzinfo=ITALY_TZ)
                if (now_it() - stamp).total_seconds() < 24 * 60 * 60:
                    await add_log(
                        f"↪️ Benvenuto già inviato — ID {user_id} — {origin}; duplicato evitato",
                        session_id=session_id,
                    )
                    return False
            username = getattr(member, "username", None)
            if username:
                person = f"@{html.escape(username)}"
            else:
                name = getattr(member, "full_name", None) or " ".join(
                    value for value in (getattr(member, "first_name", None),
                                        getattr(member, "last_name", None)) if value
                ) or "nuovo membro"
                person = f'<a href="tg://user?id={user_id}">{html.escape(name)}</a>'
            text = (
                f"👋 <b>Benvenuto {person} nella Community BestPrice24h!</b>\n\n"
                "🔥 Sei nel posto giusto per scoprire <b>offerte, ribassi di prezzo e occasioni Amazon</b> "
                "selezionate ogni giorno.\n\n"
                "📲 Le offerte vengono pubblicate sul nostro <b>canale ufficiale BestPrice24h</b>, "
                "così puoi trovarle subito senza perderti tra centinaia di prodotti.\n\n"
                "💡 <b>Il consiglio:</b> entra nel canale e attiva le notifiche per non perdere "
                "le occasioni migliori.\n\n"
                "👇 <b>Ci vediamo nel canale!</b>\n\n"
                "Se non desideri restare, premi il pulsante qui sotto: uscirai dal gruppo "
                "e non verrai invitato di nuovo automaticamente."
            )
            sent = await bot.send_message(
                chat_id=chat_id, text=text, parse_mode="HTML",
                disable_web_page_preview=True,
                reply_markup=InlineKeyboardMarkup([
                    [InlineKeyboardButton("🔥 SCOPRI LE OFFERTE", url=CHANNEL_URL)],
                    [InlineKeyboardButton("🚪 NON DESIDERO RESTARE", callback_data=f"welcome_exit:{user_id}")],
                ]),
                api_kwargs={"ephemeral_message_parameters": {"receiver_user_id": user_id}},
            )
            async with aiosqlite.connect(DB_PATH) as db:
                await db.execute(
                    "INSERT OR REPLACE INTO welcome_sent(chat_id, user_id, sent_at, message_id) VALUES (?, ?, ?, ?)",
                    (chat_id, user_id, now_it().isoformat(), sent.message_id),
                )
                await db.commit()
            await add_log(
                f"👋 BENVENUTO RISERVATO INVIATO — ID {user_id} — {origin} — messaggio {sent.message_id}",
                session_id=session_id,
            )
            return True
    except Exception as exc:
        logger.exception("Errore benvenuto per ID %s", user_id)
        await add_log(
            f"❌ BENVENUTO FALLITO — ID {user_id} — {origin} — "
            f"{type(exc).__name__}: {str(exc)[:220]}", "ERROR", session_id=session_id,
        )
        return False


async def welcome_exit(update, context):
    query = update.callback_query
    try:
        target = int(query.data.split(":", 1)[1])
    except (ValueError, IndexError):
        await query.answer("Pulsante non valido.", show_alert=True)
        return
    if query.from_user.id != target:
        await query.answer("Questo pulsante è riservato al destinatario del benvenuto.", show_alert=True)
        return
    message = query.message
    if message is None:
        await query.answer("Apri il pulsante nel gruppo.", show_alert=True)
        return
    chat_id = message.chat.id
    async with aiosqlite.connect(DB_PATH) as db:
        cursor = await db.execute("SELECT 1 FROM welcome_sent WHERE chat_id = ? AND user_id = ?", (chat_id, target))
        if not await cursor.fetchone():
            await query.answer("Benvenuto non disponibile.", show_alert=True)
            return
        await db.execute(
            "INSERT OR REPLACE INTO invitation_optout VALUES (?, ?, ?)",
            (chat_id, target, now_it().isoformat()),
        )
        await db.commit()
    await query.answer("Uscita richiesta. Non verrai invitato nuovamente.", show_alert=True)
    try:
        # Rimuove il membro senza impedirgli un futuro ingresso volontario.
        await context.bot.unban_chat_member(chat_id=chat_id, user_id=target, only_if_banned=False)
        await add_log(f"🚪 USCITA VOLONTARIA — ID {target} — gruppo {chat_id}; futuri inviti esclusi", session_id=0)
    except Exception as exc:
        await add_log(f"❌ USCITA FALLITA — ID {target} — {type(exc).__name__}: {str(exc)[:220]}", "ERROR", session_id=0)
        try:
            await context.bot.send_message(
                chat_id=chat_id,
                text="Non riesco a rimuovere il tuo account. Puoi uscire dal menu del gruppo. Non verrai invitato di nuovo.",
                api_kwargs={"ephemeral_message_parameters": {"receiver_user_id": target, "callback_query_id": query.id}},
            )
        except Exception:
            logger.exception("Impossibile notificare l'errore di uscita")


async def welcome_confirmed_invite(destination, user):
    """L'errore del benvenuto non deve annullare un'aggiunta già confermata."""
    try:
        if welcome_bot is None:
            await add_log("❌ BENVENUTO FALLITO — bot non inizializzato", "ERROR")
            return
        try:
            chat_id = utils.get_peer_id(destination)
        except (TypeError, ValueError):
            # Il gruppo B usato da InviteToChannelRequest è un supergruppo.
            saved_id = int(state["group_b"]["id"])
            chat_id = saved_id if saved_id < 0 else -1000000000000 - saved_id
        await send_welcome_once(
            welcome_bot, chat_id, user, "aggiunta confermata", session_id=current_session_id(),
        )
    except Exception as exc:
        logger.exception("Errore fallback benvenuto")
        await add_log(f"❌ BENVENUTO FALLITO — {type(exc).__name__}: {str(exc)[:220]}", "ERROR")


async def welcome_new_members(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.effective_message
    chat = update.effective_chat
    if not message or not chat or not message.new_chat_members:
        return
    if not state.get("group_b") or not _same_telegram_chat_id(chat.id, state["group_b"].get("id")):
        return
    for member in message.new_chat_members:
        await record_membership_event(message, chat, member, "JOIN")
    try:
        await context.bot.delete_message(chat_id=chat.id, message_id=message.message_id)
    except Exception as exc:
        await add_log(
            f"⚠️ Messaggio di ingresso non eliminato — {type(exc).__name__}: {str(exc)[:160]}",
            "WARNING", session_id=0,
        )
    for member in message.new_chat_members:
        await send_welcome_once(context.bot, chat.id, member, "evento Telegram", session_id=0)



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
        await add_log(f"🗑 Link eliminato nel Gruppo B — {who}", session_id=0)
    except Exception as e:
        logger.warning(
            "Impossibile eliminare messaggio con link nel Gruppo B %s: %s",
            message.message_id,
            e,
        )
        await add_log("⚠️ Link rilevato nel Gruppo B ma eliminazione non riuscita", session_id=0)

# =========================================================
# MAIN
# =========================================================

def main():

    application = (
        Application.builder()
        .token(BOT_TOKEN)
        .concurrent_updates(8)  # Gli eventi del gruppo arrivano anche durante le attese degli inviti.
        .post_init(post_init)
        .post_shutdown(post_shutdown)
        .build()
    )

    application.add_handler(CommandHandler("myid", myid, filters=filters.ChatType.PRIVATE))

    application.add_handler(
        CommandHandler(
            "start",
            start,
        )
    )

    application.add_handler(CallbackQueryHandler(welcome_exit, pattern=r"^welcome_exit:"))
    application.add_handler(CallbackQueryHandler(buttons))

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
        "V4.8.5 avviato"
    )

    application.run_polling()


if __name__ == "__main__":
    main()
