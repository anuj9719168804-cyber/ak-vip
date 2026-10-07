"""Terabox Downloader Telegram bot (Pyrogram/kurigram + aiohttp).

Send a Terabox link -> bot fetches file info through the playterabox API -> pick Download / Stream / Direct link.
"""
import asyncio
import glob
import html
import io
import json
import logging
import os
import re
import shutil
import tempfile
import time
import uuid
from urllib.parse import quote, urlsplit, urlunsplit

import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, InputUserDeactivated, MessageNotModified, PeerIdInvalid, UserIsBlocked, UserNotParticipant
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import (InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup, LinkPreviewOptions, BotCommand,
                            ReplyKeyboardMarkup, KeyboardButton)
import fallbacks
import stream_proxy

import terabox_api as tb
from store import Store

load_dotenv()
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s: %(message)s")
log = logging.getLogger("terabox-bot")


def _req(name: str, default: str = "") -> str:
    v = os.getenv(name, default).strip()
    if not v:
        raise SystemExit(f"Missing required environment variable {name} (see .env.example)")
    return v


# ---------------------------------------------------------------- config ----
# Hardcoded defaults (same as fbot's config.py) -- a real env var still overrides them.
API_ID = int(_req("API_ID", "33029767"))
API_HASH = _req("API_HASH", "5d897bed11bc8b062a12f6c1c3c5360a")
BOT_TOKEN = _req("BOT_TOKEN", "8602199507:AAFGNdLmSexulxbQ4slqS7hLv1wSRIiPIQE")
OWNER_ID = int(os.getenv("OWNER_ID", "8729304171") or 0)
ADMINS = {OWNER_ID, *(int(x) for x in re.findall(r"\d+", os.getenv("ADMINS", "8931907813")))} - {0}
LOG_CHANNEL = (os.getenv("LOG_CHANNEL", "-1004401290975") or "").strip()
FORCE_SUB = [x.strip() for x in os.getenv("FORCE_SUB", "-1004401290975").split(",") if x.strip()]
DOWNLOAD_DIR = os.getenv("DOWNLOAD_DIR", "downloads")
MAX_FILE_BYTES = int(os.getenv("MAX_FILE_SIZE_MB", "2000")) * 1024 * 1024  # Telegram bot limit is 2 GB (per part)
SPLIT_LARGE = os.getenv("SPLIT_LARGE", "1").strip().lower() not in ("0", "false", "no", "off")
# biggest file we will download to disk; anything above MAX_FILE_BYTES is split into parts
MAX_DL_BYTES = int(os.getenv("MAX_SPLIT_MB", "8192")) * 1024 * 1024 if SPLIT_LARGE else MAX_FILE_BYTES
MAX_CONCURRENT = max(1, int(os.getenv("MAX_CONCURRENT_DOWNLOADS", "3")))
MAX_PER_USER = max(1, int(os.getenv("MAX_PER_USER", "1")))
DAILY_LIMIT = int(os.getenv("DAILY_LIMIT", "0") or 0)  # 0 = unlimited
EDIT_INTERVAL = max(3.0, float(os.getenv("PROGRESS_EDIT_INTERVAL", "5")))
BOT_NAME = os.getenv("BOT_NAME", "Terabox Downloader")
MAX_FOLDER_FILES = int(os.getenv("MAX_FOLDER_FILES", "100"))
# /start welcome (same look as fbot). Empty START_PHOTO_URL = text only.
START_PHOTO_URL = os.getenv("START_PHOTO_URL", "https://t.me/log_ak_bot/202").strip()
POWERED_BY = os.getenv("POWERED_BY", "Anuj Kumar")
POWERED_BY_URL = os.getenv("POWERED_BY_URL", "https://t.me/anujedits76")
if os.getenv("TERABOX_SALT"):
    tb.SECRET_SALT = os.environ["TERABOX_SALT"]

store = Store(os.getenv("DATA_FILE", "bot_data.json"), os.getenv("MONGO_URI", "mongodb+srv://Anujedit:Anujedit@cluster0.7cs2nhd.mongodb.net/?appName=Cluster0").strip(), os.getenv("MONGO_DB_NAME", "teraboxbot"))
sem = asyncio.Semaphore(MAX_CONCURRENT)
http: aiohttp.ClientSession  # created in main()
NO_PREVIEW = LinkPreviewOptions(is_disabled=True)
PORT = int(os.environ.get("PORT", "10000"))  # Render sets PORT automatically
START_TIME = time.time()
_START_PHOTO_OK = True

# rid -> {"res": TeraResult, "uid": int, "exp": float}   (callback buttons reference results by short id)
RESULTS: dict = {}
# job id -> {"cancel": bool, "uid": int}
JOBS: dict = {}
_invite_cache: dict = {}


# --------------------------------------------------------------- helpers ----
def human_size(n: float) -> str:
    n = float(n or 0)
    for u in ("B", "KB", "MB", "GB", "TB"):
        if n < 1024 or u == "TB":
            return f"{n:.0f} {u}" if u == "B" else f"{n:.2f} {u}"
        n /= 1024


def human_time(s: float) -> str:
    s = int(max(0, s))
    h, r = divmod(s, 3600)
    m, s = divmod(r, 60)
    return f"{h}h {m}m" if h else (f"{m}m {s}s" if m else f"{s}s")


def bar(done: float, total: float, w: int = 10) -> str:
    """fbot-style hexagon progress bar."""
    p = min(1.0, done / total) if total else 0
    f = int(p * w)
    return "⬢" * f + "⬡" * (w - f)


_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ",
)
_TAG_OR_MENTION_RE = re.compile(r"(<[^>]+>|@[A-Za-z][A-Za-z0-9_]{3,31})")


def SC(text: str) -> str:
    """Small-caps the plain text of an HTML message; tags, <code> contents and @mentions stay untouched (like fbot)."""
    out, in_code = [], 0
    for part in _TAG_OR_MENTION_RE.split(text):
        if part.startswith("<") and part.endswith(">"):
            low = part.lower()
            if low.startswith("<code"):
                in_code += 1
            elif low.startswith("</code"):
                in_code = max(0, in_code - 1)
            out.append(part)
        elif part.startswith("@") or in_code:
            out.append(part)
        else:
            out.append(part.translate(_SMALLCAPS_MAP))
    return "".join(out)


try:  # coloured (blue / red) inline buttons, like fbot
    from pyrogram.enums import ButtonStyle
    BTN_PRIMARY, BTN_DANGER = ButtonStyle.PRIMARY, ButtonStyle.DANGER
except Exception:
    BTN_PRIMARY = BTN_DANGER = None


def mbtn(text: str, callback_data: str = None, url: str = None, style=None) -> Btn:
    kw = {"text": SC(text)}
    if callback_data:
        kw["callback_data"] = callback_data
    if url:
        kw["url"] = url
    if style is not None:
        try:
            return Btn(**kw, style=style)
        except TypeError:
            pass
    return Btn(**kw)


def progress_text(kind: str, name: str, done: float, total: float, t0: float, extra: str = "") -> str:
    """fbot 'Fast Downloading via Main Engine' progress card. kind = 'download' | 'upload'."""
    el = max(time.time() - t0, 1e-3)
    speed = done / el
    eta = (total - done) / speed if total and speed else 0
    pct = min(100.0, done / total * 100) if total else 0
    dl = kind == "download"
    return SC(
        f"{'📥' if dl else '📤'} <b>Fast {'Downloading' if dl else 'Uploading'} via Main Engine</b>\n\n"
        "╭━━━━❰Progress❱━➣\n"
        f"┣⪼ 🎬 File: <code>{html.escape(name)}</code>\n"
        f"{extra}"
        f"┣⪼ [{bar(done, total)}]\n"
        f"┣⪼ ✅ {pct:.1f}%\n"
        f"┣⪼ 💾 {human_size(done)} / {human_size(total)}\n"
        f"┣⪼ ⚡ {human_size(speed)}/s\n"
        f"┣⪼ 🕐 Elapsed: {human_time(el)}\n"
        f"┣⪼ ⏳ ETA: {human_time(eta)}\n"
        "╰━━━━━━━━━━━━━━━➣\n\n"
        f"⚡ Hyper {kind} connections active"
    )


def safe_name(name: str) -> str:
    name = re.sub(r'[\\/:*?"<>|\x00-\x1f]', "_", name).strip(" .") or "file"
    return name[:150]


def gc_results():
    now = time.time()
    for k in [k for k, v in RESULTS.items() if v["exp"] < now]:
        RESULTS.pop(k, None)


def is_unlimited(uid: int) -> bool:
    return is_admin(uid) or store.is_premium(uid)


def is_admin(uid: int) -> bool:
    return uid in ADMINS


def clean_url(u, allow_m3u8: bool = False):
    """Make a link safe for a Telegram URL button (percent-encode spaces/unicode, reject junk). None = don't show a button."""
    if not u or not isinstance(u, str):
        return None
    u = u.strip()
    if not u.lower().startswith(("http://", "https://")) or (not allow_m3u8 and ".m3u8" in u.lower()):
        return None
    try:
        p = urlsplit(u)
        host = p.hostname or ""
        if not host or "." not in host or host in ("localhost", "127.0.0.1"):
            return None
        out = urlunsplit((p.scheme, p.netloc, quote(p.path, safe="/%:@!$&'()*+,;=~-._"),
                          quote(p.query, safe="=&%:/?@!$'()*+,;~-._"), ""))
    except Exception:
        return None
    return out if len(out.encode()) <= 1900 else None


def _strip_url_buttons(markup):
    rows = getattr(markup, "inline_keyboard", None)
    if not rows:
        return None
    kept = [[b for b in row if not getattr(b, "url", None)] for row in rows]
    kept = [r for r in kept if r]
    return Markup(kept) if kept else None


async def safe_edit(msg, text: str, markup=None, _retry: bool = True):
    try:
        if getattr(msg, "photo", None):  # thumbnail menu is a photo message -> edit its caption
            await msg.edit_caption(text[:1024], reply_markup=markup, parse_mode=ParseMode.HTML)
        else:
            await msg.edit_text(text, reply_markup=markup, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
    except MessageNotModified:
        pass
    except FloodWait as e:
        await asyncio.sleep(min(e.value, 10))
    except Exception as e:
        log.warning("edit failed: %s", e)
        if _retry and markup is not None and any(getattr(b, "url", None) for row in (getattr(markup, "inline_keyboard", None) or []) for b in row):
            # most likely Telegram rejected a URL button (BUTTON_URL_INVALID) -> show the menu without the link buttons
            await safe_edit(msg, text, _strip_url_buttons(markup), _retry=False)


def new_user_text(u) -> str:
    """fbot-style 'New User' notice for the log channel."""
    uname = f"@{u.username}" if getattr(u, "username", None) else "(no username)"
    return ("🆕 <b>New User</b>\n\n"
            f"👤 Name: {html.escape(u.first_name or 'User')}\n"
            f"🔗 Username: {html.escape(uname)}\n"
            f"🆔 ID: <code>{u.id}</code>")


async def log_to_channel(client: Client, text: str):
    if not LOG_CHANNEL:
        return
    try:
        chat = int(LOG_CHANNEL) if re.fullmatch(r"-?\d+", LOG_CHANNEL) else LOG_CHANNEL
        await client.send_message(chat, text, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
    except Exception as e:
        log.warning("log channel failed: %s", e)


def user_tag(u) -> str:
    n = html.escape(u.first_name or "user")
    return f'<a href="tg://user?id={u.id}">{n}</a> (<code>{u.id}</code>)'


# ---------------------------------------------------------- force subscribe ----
async def _invite_link(client: Client, ref: str):
    if ref in _invite_cache:
        return _invite_cache[ref]
    link = None
    try:
        if ref.startswith("http"):
            link = ref
        elif ref.startswith("@") or not re.fullmatch(r"-?\d+", ref):
            link = f"https://t.me/{ref.lstrip('@')}"
        else:
            chat = await client.get_chat(int(ref))
            link = chat.invite_link or (f"https://t.me/{chat.username}" if chat.username else None) \
                or await client.export_chat_invite_link(int(ref))
    except Exception as e:
        log.warning("invite link for %s failed: %s", ref, e)
    _invite_cache[ref] = link
    return link


def _chat_ref(ref: str):
    m = re.search(r"t\.me/(?:\+|joinchat/)", ref)
    if m:  # private invite link: cannot check membership by link, handled as "unknown"
        return None
    m = re.search(r"t\.me/([A-Za-z0-9_]+)", ref)
    if m:
        return m.group(1)
    return int(ref) if re.fullmatch(r"-?\d+", ref) else ref.lstrip("@")


async def missing_channels(client: Client, uid: int) -> list:
    missing = []
    for ref in FORCE_SUB:
        chat = _chat_ref(ref)
        if chat is None:
            continue
        try:
            m = await client.get_chat_member(chat, uid)
            if m.status in (ChatMemberStatus.BANNED, ChatMemberStatus.LEFT):
                missing.append(ref)
        except UserNotParticipant:
            missing.append(ref)
        except Exception as e:  # bot not admin / bad ref: never lock users out because of config errors
            log.warning("force-sub check for %s failed: %s", ref, e)
    return missing


async def gate(client: Client, message) -> bool:
    """False (and a reply is sent) if the user is banned or has not joined the force-sub channels."""
    uid = message.from_user.id
    if store.is_banned(uid) and not is_admin(uid):
        await message.reply_text("🚫 You are banned from using this bot.")
        return False
    if FORCE_SUB and not is_admin(uid):
        miss = await missing_channels(client, uid)
        if miss:
            rows = []
            for i, ref in enumerate(miss, 1):
                link = await _invite_link(client, ref)
                if link:
                    rows.append([Btn(f"📢 Join Channel {i}", url=link)])
            rows.append([Btn("✅ Verify", callback_data="verify")])
            await message.reply_text(
                "🔒 <b>Access denied</b>\n\nPlease join our channel(s) first, then tap <b>Verify</b>.",
                reply_markup=Markup(rows), parse_mode=ParseMode.HTML)
            return False
    return True


# ------------------------------------------------------------- handlers ----
_SMALLCAPS_MAP = str.maketrans(
    "abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ",
    "ᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢᴀʙᴄᴅᴇғɢʜɪᴊᴋʟᴍɴᴏᴘǫʀsᴛᴜᴠᴡxʏᴢ",
)
_TAG_RE = re.compile(r"(<[^>]+>)")


def smallcaps(text: str) -> str:
    return text.translate(_SMALLCAPS_MAP)


def smallcaps_html(text: str) -> str:
    """Small-caps all plain text; HTML tags and <code>...</code> contents stay as typed."""
    out, in_code = [], 0
    for part in _TAG_RE.split(text):
        if part.startswith("<") and part.endswith(">"):
            low = part.lower()
            if low.startswith("<code"):
                in_code += 1
            elif low.startswith("</code"):
                in_code = max(0, in_code - 1)
            out.append(part)
        else:
            out.append(part if in_code else part.translate(_SMALLCAPS_MAP))
    return "".join(out)


_ME = {}  # cached bot identity (set on first /start)


def start_caption(first_name: str, bot_username: str, bot_name: str) -> str:
    name = html.escape(smallcaps(first_name or "there"))
    bot = html.escape(smallcaps(bot_name or BOT_NAME))
    head = (
        f"<b>👋 {smallcaps('Hello')} {name},</b>\n"
        f"<b>🤖 {smallcaps('I am')} <a href=\"https://t.me/{bot_username}\">{bot}</a></b>\n\n"
    )
    body = smallcaps_html(
        "⚡ I'm a very powerful Terabox downloader bot.\n\n"
        "📥 Simply send me any Terabox link, and I'll fetch the file, stream link and direct link for you in seconds.\n\n"
        "🚀 Ultra-fast processing\n"
        "🎬 Instant file extraction\n"
        "⚡ Lightning-speed downloads\n"
        "📂 Folder shares supported\n"
        "🛡️ Reliable & stable service\n"
        "🔗 Just paste your Terabox link below and let the magic begin!\n\n"
        "✅ ʏᴇ ʟɪɴᴋꜱ ꜱᴜᴘᴘᴏʀᴛᴇᴅ ʜᴀɪ:\n"
        "• <code>terabox.com</code> / <code>terafileshare.com</code>\n"
        "• <code>1024terabox.com</code> / <code>teraboxapp.com</code>\n"
        "• <code>4funbox.com</code> / <code>mirrobox.com</code>\n\n"
    )
    powered = f'<a href="{html.escape(POWERED_BY_URL, quote=True)}">{html.escape(smallcaps(POWERED_BY))}</a>'
    foot = (
        "━━━━━━━━━━━━━━━\n"
        f"👑 {smallcaps('Powered by')} {powered}\n"
        f"⚡ {smallcaps('Speed')} • {smallcaps('Performance')} • {smallcaps('Reliability')}\n"
        "━━━━━━━━━━━━━━━"
    )
    return head + body + foot


HELP_TEXT = smallcaps_html(
    "ℹ️ <b>How to use</b>\n\n"
    "🔹 <b>Just send the link:</b>\n"
    "Paste any Terabox share link directly in the chat.\n\n"
    "🔹 <b>Supported link formats:</b>\n"
    "<code>terabox.com</code>\n<code>terafileshare.com</code>\n<code>1024terabox.com</code>\n<code>teraboxapp.com</code>\n"
    "…and other Terabox mirrors\n\n"
    "📌 <b>Example:</b>\n"
    "<code>https://terasharefile.com/s/1RVsNAIpmTfurYpsJGGhG7w</code>\n\n"
    "💡 <b>Tips:</b>\n"
    f"• Files up to {human_size(MAX_FILE_BYTES)} are uploaded directly — bigger ones (up to {human_size(MAX_DL_BYTES)}) are split into parts automatically\n"
    "• Folder link? Pick one file or tap Download all\n"
    "• Use the Stream / Direct Link buttons if you just want a link\n"
    "• If a download fails, just send the link again\n"
    "• Use <code>/cancel</code> to stop an active download\n\n"
    "Having trouble? Make sure you're sending a valid Terabox share link."
)



# ------------------------------------------------- fbot-style menus ----
def rbtn(text: str, style=None):
    """Bottom (reply) keyboard button, coloured when the pyrogram build supports it."""
    if style is not None:
        try:
            return KeyboardButton(text=text, style=style)
        except TypeError:
            pass
    return text


BTN_PLANS, BTN_MYSTATUS, BTN_HELP, BTN_SUPPORT = "💎 ᴘʟᴀɴs", "📊 ᴍʏ sᴛᴀᴛᴜs", "❓ ʜᴇʟᴘ", "☎️ sᴜᴘᴘᴏʀᴛ"
MENU_BUTTON_TEXTS = [BTN_PLANS, BTN_MYSTATUS, BTN_HELP, BTN_SUPPORT]

MAIN_MENU_KB = ReplyKeyboardMarkup(
    [[rbtn(BTN_PLANS, BTN_PRIMARY), rbtn(BTN_MYSTATUS, BTN_PRIMARY)],
     [rbtn(BTN_HELP, BTN_PRIMARY), rbtn(BTN_SUPPORT, BTN_PRIMARY)]],
    resize_keyboard=True,
)


def _tolerant(expected: str) -> str:
    """Regex that ignores the invisible U+FE0F selector some clients drop/add (☎️, ❓)."""
    base = expected.replace("\ufe0f", "")
    return "^" + r"\ufe0f?".join(re.escape(c) for c in base) + r"\ufe0f?$"


def menu_text_filter(expected: str):
    return filters.regex(_tolerant(expected))


NOT_MENU_BUTTON = ~filters.regex("|".join(f"(?:{_tolerant(t)})" for t in MENU_BUTTON_TEXTS))

FALLBACK_TEXT = "👇 Apna Terabox link bhejo boss!"

PLANS = [(19, "12 Days"), (29, "21 Days"), (45, "35 Days"), (99, "99 Days"), (999, "Lifetime Access ♾️")]
PLANS_PHOTO_URL = os.getenv("PLANS_PHOTO_URL", "https://iili.io/nHyIqox.jpg")
UPI_ID = os.getenv("UPI_ID", "971916880@ybl")


def fallback_kb() -> Markup:
    """Inline [Download] [Status] buttons shown under the /start message."""
    return Markup([[mbtn("📥 Download", "fallback_download", style=BTN_PRIMARY),
                    mbtn("📊 Status", "fallback_status", style=BTN_PRIMARY)]])


def plans_text() -> str:
    lines = "\n".join(f"• ₹{p} → {d}" for p, d in PLANS)
    return smallcaps_html(
        "💎 <b>Premium Membership Plans</b>\n"
        "✨ Unlock Unlimited Access & Advanced Features!\n\n"
        f"{lines}\n\n"
        "🔒 <b>Secure Payment:</b>\n"
        f"⚡️ UPI ID: <code>{html.escape(UPI_ID)}</code>\n"
        f"🔗 QR Code: <a href=\"{html.escape(PLANS_PHOTO_URL, quote=True)}\">Scan to Pay</a>\n"
        "💡 After Payment: Send Screenshot to Admin for Instant Activation.")


def plans_kb() -> Markup:
    rows = [[mbtn(f"💎 ₹{p} - {d}", url=POWERED_BY_URL, style=BTN_PRIMARY)] for p, d in PLANS]
    rows.append([mbtn("📸 Send Payment Proof", url=POWERED_BY_URL, style=BTN_PRIMARY)])
    rows.append([mbtn("⬅️ Back", "plans_back", style=BTN_DANGER)])
    return Markup(rows)


def my_status_text(uid: int) -> str:
    u = store.data["users"].get(str(uid), {})
    pi = store.premium_info(uid)
    if is_admin(uid):
        plan = "👑 Admin (Unlimited)"
    elif pi["lifetime"]:
        plan = "💎 Premium (Lifetime ♾️)"
    elif pi["is_premium"]:
        plan = f"💎 Premium ({pi['days_left']} day{'s' if pi['days_left'] != 1 else ''} left)"
    else:
        plan = "Free"
    today = store.downloads_today(uid)
    limit = "Unlimited" if (DAILY_LIMIT == 0 or is_unlimited(uid)) else f"{today}/{DAILY_LIMIT} ({max(0, DAILY_LIMIT - today)} left)"
    return smallcaps_html(
        "<b>📊 Your Status</b>\n\n"
        f"User ID: <code>{uid}</code>\n"
        f"Plan: <code>{plan}</code>\n"
        f"Total Downloads: <code>{u.get('dl', 0)}</code>\n"
        f"Today's downloads: {limit}")


def status_kb() -> Markup:
    return Markup([[mbtn("💎 View Plans", "show_plans", style=BTN_PRIMARY)],
                   [mbtn("📞 Contact Admin", url=POWERED_BY_URL, style=BTN_PRIMARY)]])


async def send_plans(message):
    try:
        await message.reply_photo(PLANS_PHOTO_URL, caption=plans_text(), reply_markup=plans_kb(), parse_mode=ParseMode.HTML)
    except Exception as e:
        log.warning("plans photo failed, sending text: %s", e)
        await message.reply_text(plans_text(), reply_markup=plans_kb(), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


async def on_plans(client: Client, message):
    await send_plans(message)


async def on_my_status(client: Client, message):
    await message.reply_text(my_status_text(message.from_user.id), reply_markup=status_kb(), parse_mode=ParseMode.HTML)


async def on_support(client: Client, message):
    await message.reply_text(
        smallcaps_html("📞 <b>Support</b>\n\nKoi problem? Idhar baat karo:\n\n"
                       f"👤 Admin: <a href=\"{html.escape(POWERED_BY_URL, quote=True)}\">{html.escape(POWERED_BY)}</a>\n\n"
                       "⏰ 24 ghante ke andar reply, pakka!"),
        parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


async def on_fallback_download(client: Client, cq):
    await cq.answer(SC(FALLBACK_TEXT), show_alert=True)


async def on_fallback_status(client: Client, cq):
    await cq.message.reply_text(my_status_text(cq.from_user.id), reply_markup=status_kb(), parse_mode=ParseMode.HTML)
    await cq.answer()


async def on_show_plans(client: Client, cq):
    await send_plans(cq.message)
    await cq.answer()


async def on_plans_back(client: Client, cq):
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.answer()


async def on_start(client: Client, message):
    u = message.from_user
    if store.add_user(u.id, u.first_name or ""):
        await log_to_channel(client, new_user_text(u))
    if not await gate(client, message):
        return
    if not _ME:
        me = await client.get_me()
        _ME.update(username=me.username or "", name=me.first_name or BOT_NAME)
    caption = start_caption(u.first_name, _ME["username"], _ME["name"])
    global _START_PHOTO_OK
    sent_photo = False
    if START_PHOTO_URL and _START_PHOTO_OK:
        try:
            m = re.fullmatch(r"https?://t\.me/([A-Za-z0-9_]+)/(\d+)", START_PHOTO_URL)
            if m:  # t.me post link is not an image url -> copy that post (photo) with our caption
                coro = client.copy_message(message.chat.id, m.group(1), int(m.group(2)),
                                           caption=caption, parse_mode=ParseMode.HTML, reply_markup=fallback_kb())
            else:
                coro = message.reply_photo(START_PHOTO_URL, caption=caption, parse_mode=ParseMode.HTML, reply_markup=fallback_kb())
            await asyncio.wait_for(coro, timeout=12)
            sent_photo = True
        except Exception as e:
            _START_PHOTO_OK = False  # don't make every /start wait on a broken photo
            log.warning("start photo failed (disabled until restart), sending text: %s", e)
    if sent_photo:
        await message.reply_text(SC(FALLBACK_TEXT), reply_markup=MAIN_MENU_KB)
        return
    await message.reply_text(caption, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW, reply_markup=fallback_kb())
    await message.reply_text(SC(FALLBACK_TEXT), reply_markup=MAIN_MENU_KB)


async def on_help(client: Client, message):
    if await gate(client, message):
        await message.reply_text(HELP_TEXT, parse_mode=ParseMode.HTML)


# ------------------------------------------------------------------ about ----
HOSTING_TEXT = os.getenv("HOSTING_TEXT", "Dedicated High-Speed Server")


DEVELOPER_URL = "https://t.me/anujedits76"


def about_text(bot_username: str) -> str:
    bot_link = f"https://t.me/{bot_username}" if bot_username else POWERED_BY_URL
    dev = html.escape(smallcaps(POWERED_BY))
    return (
        f"💠 {smallcaps('About This Bot')} 💠\n\n"
        f"╭────[ ✨ {html.escape(smallcaps(POWERED_BY.split()[0]))} ]────⍟\n"
        f"├⍟ 🚀 {smallcaps('Bot Name')}  : <a href=\"{bot_link}\">{smallcaps('Terabox Downloader Bot')}</a>\n"
        f"├⍟ 👨‍💻 {smallcaps('Developer')}  : <a href=\"{DEVELOPER_URL}\">{dev}</a>\n"
        f"├⍟ 🔗 {smallcaps('Library')}  : <a href=\"https://docs.pyrogram.org/\">{smallcaps('Pyrogram Async')}</a>\n"
        f"├⍟ ⚡️ {smallcaps('Language')}  : <a href=\"https://www.python.org/\">{smallcaps('Python')} 3.12</a>\n"
        f"├⍟ ⚙️ {smallcaps('Database')}  : <a href=\"https://www.mongodb.com/\">{smallcaps('MongoDB')}</a>\n"
        f"├⍟ ⭐️ {smallcaps('Hosting')}  :  {smallcaps(HOSTING_TEXT)}\n"
        "╰───────────────⍟"
    )


async def on_about(client: Client, message):
    if not _ME:
        me = await client.get_me()
        _ME.update(username=me.username or "", name=me.first_name or BOT_NAME)
    await message.reply_text(about_text(_ME["username"]), parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW,
                             reply_markup=Markup([[mbtn("❌ Close", "about_close", style=BTN_DANGER)]]))


async def on_about_close(client: Client, cq):
    try:
        await cq.message.delete()
    except Exception:
        pass
    await cq.answer()


async def on_cancel(client: Client, message):
    n = 0
    for j in JOBS.values():
        if j["uid"] == message.from_user.id and not j["cancel"]:
            j["cancel"] = True
            n += 1
    await message.reply_text("🛑 Cancelling…" if n else "No active download.")


async def on_verify(client: Client, cq):
    miss = await missing_channels(client, cq.from_user.id)
    if miss:
        await cq.answer("❌ You have not joined all channels yet.", show_alert=True)
        return
    await cq.answer("✅ Verified!")
    try:
        await cq.message.edit_text("✅ <b>Verified!</b> Now send me a Terabox link.", parse_mode=ParseMode.HTML)
    except Exception:
        pass


PAGE_SIZE = 10


def file_menu_kb(rid: str, idx: int, f: tb.TeraFile, back: bool = False) -> Markup:
    """fbot-style 'Choose an action' keyboard: Download / Stream Link / Direct Link / Cancel (or Back to list)."""
    rows = [[mbtn("🔽 Download", f"dl:{rid}:{idx}", style=BTN_PRIMARY)],
            [mbtn("🔗 Stream Link", f"st:{rid}:{idx}", style=BTN_PRIMARY)]]
    if f.download_url:  # Changed to callback instead of URL button
        rows.append([mbtn("📥 Direct Link", f"dr:{rid}:{idx}", style=BTN_PRIMARY)])
    if back:
        rows.append([mbtn("⬅️ Back to list", f"p:{rid}:{idx // PAGE_SIZE}", style=BTN_DANGER)])
    else:
        rows.append([mbtn("❌ Cancel", f"cx:{rid}", style=BTN_DANGER)])
    return Markup(rows)


def stream_link_for(f: tb.TeraFile) -> str:
    """Playable link for the Stream button: our own /stream proxy for plain files (seekable), else the raw stream url."""
    # Prefer direct stream/download URLs that aren't HLS
    src = next((u for u in (f.stream_url, f.download_url) if u and u.startswith("http") and ".m3u8" not in u.lower()), None)
    if src:
        try:
            u = stream_proxy.register_stream(src, f.name, f.size)
            if u:
                log.debug("Stream proxy registered for %s: %s", f.name[:50], u)
                return u
            else:
                log.warning("Stream proxy registration failed (no public URL) for %s", f.name[:50])
        except Exception as e:
            log.error("Error registering stream for %s: %s", f.name[:50], e)
    
    # Fallback to raw stream/m3u8 URL
    fallback = f.stream_url or f.m3u8_url or ""
    if fallback:
        log.debug("Using fallback stream URL for %s: %s", f.name[:50], fallback[:100])
    else:
        log.warning("No stream URL available for %s", f.name[:50])
    return fallback


def stream_card(rid: str, idx: int, f: tb.TeraFile, back: bool, su: str = ""):
    """fbot-style: 'Stream Link Ready' with just one 🔗 Open Stream button."""
    try:
        su = clean_url(su or stream_link_for(f), allow_m3u8=True)
        if su:
            return (SC(f"<b>Stream Link Ready</b>\n\nName: <code>{html.escape(f.name)}</code>\nSize: <code>{human_size(f.size)}</code>"),
                    Markup([[mbtn("🔗 Open Stream", url=su, style=BTN_PRIMARY)]]))
    except Exception as e:
        log.error("Error in stream_card for %s: %s", f.name[:50], e)
    
    back_cb = f"f:{rid}:{idx}" if back else f"m:{rid}:{idx}"
    msg = (
        "<b>❌ No stream link found for this file</b>\n\n"
        "This can happen if:\n"
        "• The file is encrypted/protected\n"
        "• The share link has expired\n"
        "• The stream service is temporarily unavailable\n\n"
        "Try using <b>Direct Link</b> instead."
    )
    return SC(msg), Markup([[mbtn("⬅️ Back", back_cb, style=BTN_DANGER)]])


_CATEGORIES = (
    ("Video", {"mp4", "mkv", "mov", "avi", "webm", "m4v", "ts", "flv", "3gp", "wmv", "mpg", "mpeg", "m3u8"}),
    ("Audio", {"mp3", "m4a", "wav", "flac", "aac", "ogg", "opus", "wma"}),
    ("Image", {"jpg", "jpeg", "png", "gif", "webp", "bmp", "heic", "svg"}),
    ("Archive", {"zip", "rar", "7z", "tar", "gz", "bz2", "xz", "iso"}),
    ("Document", {"pdf", "doc", "docx", "xls", "xlsx", "ppt", "pptx", "txt", "epub", "csv", "json", "md"}),
    ("App", {"apk", "exe", "msi", "dmg", "deb"}),
)


def file_category(name: str) -> str:
    """'movie.MP4' -> 'Video', 'a.zip' -> 'Archive', unknown/no extension -> 'File'."""
    ext = (name or "").rsplit(".", 1)[-1].lower() if "." in (name or "") else ""
    return next((cat for cat, exts in _CATEGORIES if ext in exts), "File")


def meta_block(f: tb.TeraFile, url: str = "") -> str:
    """fbot-style info block: title + blockquote with file details (only lines we actually know)."""
    lines = [f"📄 {smallcaps('File Name')}: {html.escape(smallcaps(f.name[:90]))}",
             f"📦 {smallcaps('Size')}: {human_size(f.size)}"]
    if f.duration:
        h, r = divmod(int(f.duration), 3600)
        m, sec = divmod(r, 60)
        lines.append(f"⏱️ {smallcaps('Duration')}: " + (f"{h}:{m:02d}:{sec:02d}" if h else f"{m}:{sec:02d}"))
    lines.append(f"🏷️ {smallcaps('Category')}: {html.escape(smallcaps(file_category(f.name)))}")
    if f.ctime > 946684800:
        lines.append(f"📅 {smallcaps('Uploaded')}: {time.strftime('%Y-%m-%d', time.gmtime(f.ctime))}")
    if url.startswith("http"):
        lines.append(f"🔗 {smallcaps('Source')}: Terabox")
    icon = "🎬" if f.is_video else "📄"
    return f"{icon} <b>{html.escape(smallcaps(f.name[:90].rsplit('.', 1)[0]))}</b>\n\n<blockquote>{chr(10).join(lines)}</blockquote>"


def menu_text(url: str, f: tb.TeraFile = None) -> str:
    head = f"<b>{smallcaps('Link received')}</b>\n<code>{html.escape(url)}</code>\n\n"
    if not f:
        return head + smallcaps("Choose an action:")
    return head + meta_block(f, url) + f"\n\n{smallcaps('Choose an action:')}"


def file_card(f: tb.TeraFile) -> str:
    return meta_block(f) + f"\n\n{smallcaps('Choose an action:')}"


async def fetch_thumb(url: str):
    """Download a thumbnail ourselves (Telegram often cannot fetch CDN urls). Returns BytesIO or None."""
    if not url or not url.startswith("http"):
        return None
    try:
        async with http.get(url, headers=DL_HEADERS, timeout=aiohttp.ClientTimeout(total=6, connect=4)) as r:
            if r.status != 200 or "image" not in (r.headers.get("Content-Type") or ""):
                return None
            data = await r.content.read(5 * 1024 * 1024)
        bio = io.BytesIO(data)
        bio.name = "thumb.jpg"
        return bio if len(data) > 500 else None
    except BaseException as e:  # never let a thumbnail problem break the menu (but still honour task cancellation)
        if isinstance(e, asyncio.CancelledError):
            raise
        log.info("thumbnail skipped (%s): %s", url[:80], e.__class__.__name__)
        return None


async def _frame_from(src: str, hdr: str):
    """Duration via ffprobe, then one frame at 10% of the video - the same spot make_thumb() uses for the sent video."""
    net = ["-headers", hdr, "-reconnect", "1", "-reconnect_streamed", "1", "-reconnect_delay_max", "2"]
    dur = 0.0
    rc, out = await _run("ffprobe", "-v", "error", *net, "-show_entries", "format=duration", "-of", "csv=p=0", src, timeout=8)
    if rc == 0:
        try:
            dur = float(out.decode().strip().splitlines()[0])
        except Exception:
            dur = 0.0
    at = max(1, int(dur * 0.1)) if dur else 8
    vf = "scale='if(gt(iw,ih),960,-2)':'if(gt(iw,ih),-2,960)':flags=lanczos"
    rc, img = await _run("ffmpeg", "-nostdin", "-loglevel", "error", *net, "-ss", str(at), "-i", src, "-frames:v", "1",
                         "-vf", vf, "-q:v", "4", "-f", "image2pipe", "-vcodec", "mjpeg", "-", timeout=12)
    return img if rc == 0 and len(img) > 2000 else None


async def frame_thumb(f: tb.TeraFile):
    """Sharp menu preview: a frame cut from the remote video with ffmpeg at the same spot (10%) as the thumbnail of the
    sent video, so both look alike. Tries the stream and download links. Returns BytesIO or None."""
    if not (shutil.which("ffmpeg") and shutil.which("ffprobe")) or not f.is_video:
        return None
    hdr = f"User-Agent: {DL_HEADERS['User-Agent']}\r\nReferer: {DL_HEADERS['Referer']}\r\n"
    srcs = list(dict.fromkeys(u for u in (f.download_url, f.stream_url) if u and u.startswith("http") and ".m3u8" not in u.lower()))

    async def run():
        for src in srcs:
            img = await _frame_from(src, hdr)
            if img:
                return img
        return None
    try:
        img = await asyncio.wait_for(run(), timeout=FRAME_TIMEOUT)
    except Exception:
        return None
    if not img:
        return None
    bio = io.BytesIO(img)
    bio.name = "thumb.jpg"
    return bio


FRAME_TIMEOUT = 20  # max seconds to wait for the sharp video frame before falling back to the API thumbnail


THUMB_HD = os.getenv("THUMB_HD", "1") == "1"  # 1 = sharper thumbnail (bigger size, re-compressed so it still loads fast); 0 = as Terabox gives it
THUMB_MAX_SIDE = int(os.getenv("THUMB_MAX_SIDE", "800"))      # px - plenty for a chat card, loads fast even on slow mobile data
THUMB_MAX_KB = int(os.getenv("THUMB_MAX_KB", "0"))        # 0 = never re-compress: full HD exactly as Terabox gives it (default). e.g. 150 = shrink bigger thumbnails to <=150 KB


async def shrink_thumb(bio):
    """Big thumbnails load slowly on weak mobile data (half-loaded card). Re-compress to <= THUMB_MAX_SIDE px / small JPEG."""
    try:
        data = bio.getvalue()
        if THUMB_MAX_KB <= 0 or len(data) <= THUMB_MAX_KB * 1024 or not shutil.which("ffmpeg"):
            return bio
        with tempfile.TemporaryDirectory() as d:
            src, dst = os.path.join(d, "in.img"), os.path.join(d, "out.jpg")
            with open(src, "wb") as fh:
                fh.write(data)
            vf = f"scale='if(gt(iw,ih),min(iw,{THUMB_MAX_SIDE}),-2)':'if(gt(iw,ih),-2,min(ih,{THUMB_MAX_SIDE}))':flags=lanczos"
            best = None
            for q in ("2", "3", "5", "8", "12", "18"):  # raise compression until it fits the size limit
                rc, _ = await _run("ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", src, "-vf", vf, "-frames:v", "1", "-q:v", q, dst, timeout=15)
                if rc != 0 or not os.path.exists(dst):
                    break
                with open(dst, "rb") as fh:
                    best = fh.read()
                if len(best) <= THUMB_MAX_KB * 1024:
                    break
            if best and 2000 < len(best) < len(data):
                log.info("thumbnail shrunk %d KB -> %d KB", len(data) // 1024, len(best) // 1024)
                nb = io.BytesIO(best)
                nb.name = "thumb.jpg"
                return nb
        return bio
    except Exception as e:
        log.info("thumbnail shrink skipped: %s", e)
        return bio


_THUMB_SIZE = re.compile(r"size=[A-Za-z0-9_]+")
THUMB_UPSIZE = ("c1280_u960", "c850_u580")  # bigger Terabox thumbnail variants to try (largest valid one wins)


async def fetch_thumb_hd(url: str):
    """Link thumbnail in the best quality Terabox will give: tries bigger size variants of the same thumbnail URL
    next to the original and keeps the one with the most data. Returns BytesIO or None."""
    if not url or not url.startswith("http"):
        return None
    urls = [url]
    if _THUMB_SIZE.search(url):
        urls += [_THUMB_SIZE.sub("size=" + s, url, count=1) for s in THUMB_UPSIZE]
    got = await asyncio.gather(*(fetch_thumb(u) for u in urls), return_exceptions=True)
    best, best_len, best_url = None, 0, ""
    for u, g in zip(urls, got):
        if g and not isinstance(g, BaseException):
            n = len(g.getbuffer())
            if n > best_len:
                best, best_len, best_url = g, n, u
    if best is not None:
        log.info("link thumbnail: %d KB (%s)", best_len // 1024, (_THUMB_SIZE.search(best_url) or [""])[0] if _THUMB_SIZE.search(best_url) else "original")
        best.seek(0)
    return best


MENU_THUMB = os.getenv("MENU_THUMB", "link").lower()  # "link" = the link's own thumbnail (default), "frame" = frame cut from the video


async def get_menu_thumb(f: tb.TeraFile):
    """Thumbnail for the file card. Default: the link's own Terabox thumbnail (the same picture the share link shows);
    a frame cut from the video is only used when the link has no thumbnail (or MENU_THUMB=frame puts it first)."""
    if MENU_THUMB == "frame":
        order = [("video frame", lambda: frame_thumb(f)), ("link thumbnail", lambda: (fetch_thumb_hd(f.thumb) if THUMB_HD else fetch_thumb(f.thumb)))]
    else:
        order = [("link thumbnail", lambda: (fetch_thumb_hd(f.thumb) if THUMB_HD else fetch_thumb(f.thumb))), ("video frame", lambda: frame_thumb(f))]
    for name, fn in order:
        try:
            img = await fn()
        except Exception:
            img = None
        if img:
            if THUMB_HD:
                img = await shrink_thumb(img)
            img.seek(0)
            log.info("menu thumbnail: %s", name)
            return img
    log.info("menu thumbnail: none")
    return None


THUMB_WAIT_KBPS = float(os.getenv("THUMB_WAIT_KBPS", "60"))  # assumed phone download speed (KB/s) used to time the info card
THUMB_WAIT_MAX = float(os.getenv("THUMB_WAIT_MAX", "20"))    # never hold the info back longer than this (seconds)


async def send_photo_card(target, thumb, text: str, kb) -> bool:
    """Send the HD thumbnail FIRST (info held back), give the phone time to load it, then add the info + buttons
    to the same message. The bot cannot see when a phone finished loading, so the wait is estimated from the image
    size (THUMB_WAIT_KBPS) and capped (THUMB_WAIT_MAX); THUMB_WAIT_KBPS=0 sends info together with the photo."""
    try:
        kb_size = len(thumb.getbuffer()) / 1024
        wait = 0.0 if THUMB_WAIT_KBPS <= 0 else min(THUMB_WAIT_MAX, max(2.0, kb_size / THUMB_WAIT_KBPS))
        if wait <= 0:
            await asyncio.wait_for(target.reply_photo(thumb, caption=text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb), timeout=30)
            return True
        m = await asyncio.wait_for(target.reply_photo(thumb, caption=SC("🖼 <b>Loading preview…</b>"), parse_mode=ParseMode.HTML), timeout=30)
    except Exception as e:
        log.warning("thumbnail card failed: %s", e)
        return False
    log.info("thumbnail sent (%d KB), info after %.1fs", kb_size, wait)
    await asyncio.sleep(wait)
    try:
        await m.edit_caption(text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb)
    except Exception as e:
        log.warning("adding info to thumbnail card failed (%s), sending it as a new card", e)
        try:
            await m.delete()
        except Exception:
            pass
        try:
            thumb.seek(0)
            await target.reply_photo(thumb, caption=text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb)
        except Exception as e2:
            log.warning("thumbnail card resend failed: %s", e2)
            await target.reply_text(text, parse_mode=ParseMode.HTML, reply_markup=kb)
    return True


async def on_link(client: Client, message):
    u = message.from_user
    if store.add_user(u.id, u.first_name or ""):
        await log_to_channel(client, new_user_text(u))
    url = tb.extract_terabox_url(message.text or "")
    if not url:
        if message.chat.type.name == "PRIVATE":
            await message.reply_text("❌ Please send a valid Terabox link.")
        return
    if not await gate(client, message):
        return
    status = await message.reply_text("🔍 <b>Fetching file info…</b>", parse_mode=ParseMode.HTML)
    try:
        res = await asyncio.wait_for(tb.fetch_terabox(url, session=http), timeout=90)
    except Exception as e:
        reason = str(e) or e.__class__.__name__
        log.warning("fetch failed for %s: %s", url, reason)
        extra = f"\n\n<code>{html.escape(reason[:350])}</code>" if is_admin(u.id) else ""
        await safe_edit(status, "❌ <b>Could not fetch this link.</b>\nIt may be invalid, private or deleted. Try again later." + extra)
        await log_to_channel(client, f"⚠️ <b>Fetch failed</b>\n{user_tag(u)}\n<code>{html.escape(url)}</code>\n<code>{html.escape(str(e)[:300])}</code>")
        return

    try:
        skipped = 0
        if any(f.is_dir for f in res.files):  # folder share: walk the tree and flatten to a file list
            await safe_edit(status, "📂 <b>Folder detected — scanning files…</b>")
            files, skipped = await tb.list_folder(url, res, session=http, max_files=MAX_FOLDER_FILES)
            if not files:
                await safe_edit(status, "📂 This folder has no downloadable files.")
                return
            res = tb.TeraResult(title=res.title, files=files)

        if len(res.files) == 1 and not skipped and not res.files[0].is_dir:
            res.files[0].source = url.split("?")[0]  # single-file share: allows switching to another server on a bad link
        gc_results()
        rid = uuid.uuid4().hex[:8]
        RESULTS[rid] = {"res": res, "uid": u.id, "exp": time.time() + 3600, "skipped": skipped, "url": url}
        if len(res.files) == 1:
            f = res.files[0]
            text, kb = menu_text(url, f), file_menu_kb(rid, 0, f)
            thumb = await get_menu_thumb(f)  # info + thumbnail go out together (no text-then-photo flicker)
            sent = bool(thumb) and await send_photo_card(message, thumb, text, kb)
            if sent:
                try:
                    await status.delete()
                except Exception:
                    pass
            else:
                await safe_edit(status, text, kb)
        else:
            text, kb = folder_page(rid, 0)
            await safe_edit(status, text, kb)
    except Exception as e:
        log.exception("on_link failed after fetch for %s", url)
        extra = f"\n\n<code>{html.escape(f'{e.__class__.__name__}: {e}'[:300])}</code>" if is_admin(u.id) else ""
        await safe_edit(status, "❌ <b>Something went wrong while preparing this file.</b>\nPlease try again." + extra)
        await log_to_channel(client, f"⚠️ <b>on_link error</b>\n{user_tag(u)}\n<code>{html.escape(url)}</code>\n<code>{html.escape(f'{e.__class__.__name__}: {e}'[:300])}</code>")


def folder_page(rid: str, page: int):
    ent = RESULTS[rid]
    files = ent["res"].files
    pages = max(1, (len(files) + PAGE_SIZE - 1) // PAGE_SIZE)
    page = max(0, min(page, pages - 1))
    start = page * PAGE_SIZE
    total = sum(f.size for f in files)
    head = f"📂 <b>{len(files)} files</b> • {human_size(total)} total • page {page + 1}/{pages}\n"
    if ent.get("skipped"):
        head += f"<i>({ent['skipped']} sub-folders were not scanned — limit reached)</i>\n"
    lines = [head]
    rows, row = [], []
    for j, f in enumerate(files[start:start + PAGE_SIZE]):
        n = start + j + 1
        lines.append(f"{n}. {html.escape(f.name[:55])} — <i>{human_size(f.size)}</i>")
        row.append(Btn(str(n), callback_data=f"f:{rid}:{start + j}"))
        if len(row) == 5:
            rows.append(row)
            row = []
    if row:
        rows.append(row)
    nav = []
    if page > 0:
        nav.append(Btn("◀️ Prev", callback_data=f"p:{rid}:{page - 1}"))
    if page < pages - 1:
        nav.append(Btn(f"Next ▶️ ({page + 2}/{pages})", callback_data=f"p:{rid}:{page + 1}"))
    if nav:
        rows.append(nav)
    rows.append([Btn(f"⬇️ Download all ({len(files)})", callback_data=f"all:{rid}")])
    return "\n".join(lines) + "\n\nTap a number to open a file.", Markup(rows)


async def on_page(client: Client, cq):
    _, rid, page = cq.data.split(":")
    ent = RESULTS.get(rid)
    if not ent or ent["exp"] < time.time() or (ent["uid"] != cq.from_user.id and not is_admin(cq.from_user.id)):
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    await cq.answer()
    text, kb = folder_page(rid, int(page))
    if getattr(cq.message, "photo", None):  # leaving a photo file card -> fresh text list, drop the photo card
        try:
            await cq.message.reply_text(text, reply_markup=kb, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)
            await cq.message.delete()
            return
        except Exception as e:
            log.warning("could not swap photo card for list: %s", e)
    await safe_edit(cq.message, text, kb)


def _get_file(rid: str, idx: int, uid: int):
    ent = RESULTS.get(rid)
    if not ent or ent["exp"] < time.time():
        return None
    if ent["uid"] != uid and not is_admin(uid):
        return None
    try:
        return ent["res"].files[idx]
    except IndexError:
        return None


async def on_file_pick(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    f = _get_file(rid, int(idx), cq.from_user.id)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    await cq.answer()
    multi = len(RESULTS[rid]["res"].files) > 1
    text, kb = file_card(f), file_menu_kb(rid, int(idx), f, back=multi)
    if getattr(cq.message, "photo", None):  # already a photo card (e.g. Back from the stream card)
        await safe_edit(cq.message, text, kb)
        return
    # coming from the text file list: show the file card WITH its thumbnail, same as a single link
    await safe_edit(cq.message, SC("🔍 <b>Loading preview…</b>"))
    thumb = await get_menu_thumb(f)
    if thumb and await send_photo_card(cq.message, thumb, text, kb):
        try:
            await cq.message.delete()
        except Exception:
            pass
        return
    await safe_edit(cq.message, text, kb)


STREAM_DEADLINE = 12.0  # max seconds to wait while racing resolvers for the stream link


async def fastest_stream_link(f: tb.TeraFile) -> str:
    """Race the current link + every fallback resolver, speed-test each playable (non-HLS) link
    and return the fastest one. HLS (.m3u8) is used only if nothing else works."""
    async def probe_cand(name: str, cand):
        if not cand:
            return []
        out = []
        for u in dict.fromkeys(x for x in (cand.stream_url, cand.download_url) if x and x.startswith("http")):
            if ".m3u8" in u.lower():
                continue
            out.append((await probe_speed(u), name, u))
        return out

    async def via_resolver(name: str, fn):
        res = await asyncio.wait_for(fn(f.source, http), timeout=STREAM_DEADLINE)
        if len(res.files) != 1:
            return []
        c = res.files[0]
        if f.size and c.size and abs(c.size - f.size) > 0.03 * f.size:  # not the same file
            return []
        return await probe_cand(name, c)

    jobs = [asyncio.create_task(probe_cand("current", f))]
    if f.source:
        jobs += [asyncio.create_task(via_resolver(n, fn)) for n, fn in fallbacks.FALLBACKS]
    done, pending = await asyncio.wait(jobs, timeout=STREAM_DEADLINE)
    for t in pending:
        t.cancel()
    found = []
    for t in done:
        try:
            found += [r for r in t.result() if r[0] > 0]
        except Exception:
            continue
    found.sort(key=lambda x: -x[0])
    log.info("stream ranking: %s", ", ".join(f"{n}={human_size(sp)}/s" for sp, n, _ in found) or "none usable")
    if found:
        link = found[0][2]
        try:
            return stream_proxy.register_stream(link, f.name, f.size) or link  # proxy = seekable; raw link if no public URL
        except Exception as e:
            log.warning("stream proxy registration failed: %s", e)
            return link
    return f.m3u8_url or ""  # last resort: HLS playlist if the API gave one


async def on_stream(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    f = _get_file(rid, int(idx), cq.from_user.id)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    
    await cq.answer()
    ent = RESULTS.get(rid)
    if not ent:
        await safe_edit(cq.message, SC("❌ Error: File data not found."))
        return
    
    multi = len(ent["res"].files) > 1
    
    # ALWAYS fetch fresh from API - no caching
    await safe_edit(cq.message, SC("🔍 <b>Finding the fastest stream link…</b>"))
    su = ""
    
    if ent.get("url"):  # single links AND folder files: pick a really playable link (speed-tested) and serve it inline
        try:
            log.info("🔄 Stream button: finding fastest stream for %s", ent["url"][:80])
            su = await fastest_stream_link(f)
            if su:
                log.info("✅ Stream API returned: %s", su[:100])
            else:
                log.warning("❌ Stream API returned empty for %s", ent["url"][:80])
        except Exception as e:
            log.error("❌ Stream API error: %s", e)
            await safe_edit(cq.message, SC(f"⚠️ <b>Stream API Error</b>\n\n<code>{str(e)[:200]}</code>\n\nTry Direct Link instead."))
            return
    
    try:
        text, kb = stream_card(rid, int(idx), f, back=multi, su=su)
    except Exception as e:
        log.error("Error generating stream card for rid:%s: %s", rid, e)
        text = SC("❌ Error generating stream card")
        kb = Markup([[mbtn("⬅️ Back", f"f:{rid}:{idx}", style=BTN_DANGER)]])
    
    await safe_edit(cq.message, text, kb)


async def on_menu_back(client: Client, cq):
    """Back from the stream card on a single-file link -> 'Link received' menu."""
    _, rid, idx = cq.data.split(":")
    f = _get_file(rid, int(idx), cq.from_user.id)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    await cq.answer()
    await safe_edit(cq.message, menu_text(RESULTS[rid].get("url", ""), f), file_menu_kb(rid, int(idx), f))


async def on_cancel_menu(client: Client, cq):
    RESULTS.pop(cq.data.split(":", 1)[1], None)
    await cq.answer("Cancelled")
    await safe_edit(cq.message, SC("❌ <b>Cancelled</b>"))


async def on_cancel_btn(client: Client, cq):
    job = JOBS.get(cq.data.split(":", 1)[1])
    if job and (job["uid"] == cq.from_user.id or is_admin(cq.from_user.id)):
        job["cancel"] = True
        await cq.answer("🛑 Cancelling…")
    else:
        await cq.answer("Job not found.", show_alert=True)


class Cancelled(Exception):
    pass


class SlowLink(Exception):
    """Link is too slow (or unusable) AND an alternative link can be tried."""


SLOW_BYTES_S = 150 * 1024
SLOW_GRACE_S = 12


def check_slow(job: dict, t0: float, done: int):
    if job.get("slow_check"):
        el = time.time() - t0
        if el >= SLOW_GRACE_S and done / max(el, 1e-3) < SLOW_BYTES_S:
            raise SlowLink(f"link too slow ({human_size(done / el)}/s)")


async def hls_download(url: str, dest: str, job: dict, total_hint: int, on_progress) -> int:
    """HLS (.m3u8) -> mp4 with ffmpeg stream copy. Progress = bytes written so far."""
    if not shutil.which("ffmpeg"):
        raise RuntimeError("this link is an HLS stream and ffmpeg is not installed on the server")
    proc = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y",
        *(["-user_agent", "Mozilla/5.0"] if url.lower().startswith("http") else []), "-i", url,
        "-c", "copy", "-bsf:a", "aac_adtstoasc", "-movflags", "+faststart", dest,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.PIPE)
    last_size, last_growth, last_edit = 0, time.time(), 0.0
    try:
        while proc.returncode is None:
            try:
                await asyncio.wait_for(proc.wait(), timeout=2)
            except asyncio.TimeoutError:
                pass
            if job["cancel"]:
                raise Cancelled()
            size = os.path.getsize(dest) if os.path.exists(dest) else 0
            if size > MAX_DL_BYTES:
                raise RuntimeError("TOO_BIG")
            now = time.time()
            if size > last_size:
                last_size, last_growth = size, now
            elif now - last_growth > 120:
                raise RuntimeError("stream stalled (no data for 120s)")
            if now - last_edit >= EDIT_INTERVAL:
                last_edit = now
                await on_progress(size, total_hint)
        if proc.returncode != 0:
            err = (await proc.stderr.read()).decode(errors="ignore").strip().splitlines()
            raise RuntimeError("ffmpeg failed: " + (err[-1][:150] if err else f"exit {proc.returncode}"))
    finally:
        if proc.returncode is None:
            proc.kill()
            await proc.wait()
    return os.path.getsize(dest)


DL_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0 Safari/537.36",
    "Referer": "https://playterabox.com/",
}
DL_TIMEOUT = aiohttp.ClientTimeout(total=None, connect=30, sock_read=90)  # sock_read = stall detector
PARALLEL_CONNECTIONS = max(1, int(os.getenv("PARALLEL_CONNECTIONS", "8")))
PARALLEL_MIN_BYTES = 20 * 1024 * 1024  # smaller files are not worth splitting
CHUNK = 256 * 1024


async def _probe_ranges(url: str):
    """Returns total size if the server honours Range requests, else 0."""
    try:
        async with http.get(url, headers={**DL_HEADERS, "Range": "bytes=0-0"}, timeout=DL_TIMEOUT, allow_redirects=True) as r:
            if r.status != 206 or "text/html" in (r.headers.get("Content-Type") or "").lower():
                return 0
            m = re.fullmatch(r"bytes 0-0/(\d+)", (r.headers.get("Content-Range") or "").strip())
            return int(m.group(1)) if m else 0
    except Exception:
        return 0


async def parallel_download(url: str, dest: str, job: dict, total: int, on_progress) -> int:
    """Split `total` bytes over PARALLEL_CONNECTIONS ranged GETs written into one preallocated file.
    A broken range worker is retried (resuming where it stopped); any other failure bubbles up so the caller can fall back."""
    n = min(PARALLEL_CONNECTIONS, max(1, total // (4 * 1024 * 1024)))
    part = -(-total // n)
    ranges = [(i * part, min((i + 1) * part, total) - 1) for i in range(n) if i * part < total]
    state = {"done": 0}
    with open(dest, "wb") as fh:
        fh.truncate(total)

    async def worker(fh, start: int, end: int):
        pos, fails = start, 0
        while pos <= end:
            before = pos
            try:
                async with http.get(url, headers={**DL_HEADERS, "Range": f"bytes={pos}-{end}"}, timeout=DL_TIMEOUT) as r:
                    if r.status != 206:
                        raise RuntimeError(f"range request answered HTTP {r.status}")
                    async for chunk in r.content.iter_chunked(CHUNK):
                        if job["cancel"]:
                            raise Cancelled()
                        chunk = chunk[: end - pos + 1]
                        fh.seek(pos)  # no await between seek and write -> safe across coroutines
                        fh.write(chunk)
                        pos += len(chunk)
                        state["done"] += len(chunk)
                        if pos > end:
                            break
                if pos == before:  # server answered but sent nothing -> count it, don't spin forever
                    raise RuntimeError("range request returned no data")
            except Cancelled:
                raise
            except Exception:
                fails += 1
                if fails > 3:
                    raise
                await asyncio.sleep(1.5 * fails)

    with open(dest, "r+b") as fh:
        tasks = [asyncio.create_task(worker(fh, a, b)) for a, b in ranges]
        try:
            last, t0 = 0.0, time.time()
            while not all(t.done() for t in tasks):
                await asyncio.wait(tasks, timeout=1, return_when=asyncio.FIRST_EXCEPTION)
                for t in tasks:
                    if t.done() and t.exception():
                        raise t.exception()
                check_slow(job, t0, state["done"])
                now = time.time()
                if now - last >= EDIT_INTERVAL:
                    last = now
                    await on_progress(state["done"], total)
            for t in tasks:
                if t.exception():
                    raise t.exception()
        finally:
            for t in tasks:
                t.cancel()
            await asyncio.gather(*tasks, return_exceptions=True)
    if state["done"] != total:
        raise RuntimeError(f"incomplete download ({state['done']}/{total} bytes)")
    return total


async def sequential_download(url: str, dest: str, job: dict, total_hint: int, on_progress) -> int:
    async with http.get(url, headers=DL_HEADERS, timeout=DL_TIMEOUT, allow_redirects=True) as r:
        if r.status >= 400:
            raise RuntimeError(f"download server returned HTTP {r.status}")
        if "text/html" in (r.headers.get("Content-Type") or "").lower():
            raise RuntimeError("download link expired or blocked (got a web page)")
        total = int(r.headers.get("Content-Length") or 0) or total_hint
        if total and total > MAX_DL_BYTES:
            raise RuntimeError("TOO_BIG")
        done, last, t0 = 0, 0.0, time.time()
        with open(dest, "wb") as fh:
            async for chunk in r.content.iter_chunked(CHUNK):
                if job["cancel"]:
                    raise Cancelled()
                fh.write(chunk)
                done += len(chunk)
                check_slow(job, t0, done)
                if done > MAX_DL_BYTES:
                    raise RuntimeError("TOO_BIG")
                now = time.time()
                if now - last >= EDIT_INTERVAL:
                    last = now
                    await on_progress(done, total)
    return done


async def download_file(url: str, dest: str, job: dict, total_hint: int, on_progress):
    if ".m3u8" in url.lower():
        return await hls_download(url, dest, job, total_hint, on_progress)
    if PARALLEL_CONNECTIONS > 1:
        total = await _probe_ranges(url)
        if total >= PARALLEL_MIN_BYTES:
            if total > MAX_DL_BYTES:
                raise RuntimeError("TOO_BIG")
            try:
                return await parallel_download(url, dest, job, total, on_progress)
            except (Cancelled, SlowLink, asyncio.CancelledError):
                raise
            except Exception as e:  # server misbehaved mid-way: start over the plain way
                log.warning("parallel download failed (%s) - falling back to single connection", e)
    return await sequential_download(url, dest, job, total_hint, on_progress)


async def precheck(client: Client, cq, need: int = 1) -> bool:
    """Common ban / force-sub / limit / per-user checks for download buttons. Answers the callback on failure."""
    uid = cq.from_user.id
    if store.is_banned(uid) and not is_admin(uid):
        await cq.answer("🚫 Banned.", show_alert=True)
        return False
    if FORCE_SUB and not is_admin(uid) and await missing_channels(client, uid):
        await cq.answer("🔒 Join the channel(s) first, then send /start.", show_alert=True)
        return False
    if DAILY_LIMIT and not is_unlimited(uid) and store.downloads_today(uid) + need > DAILY_LIMIT:
        left = max(0, DAILY_LIMIT - store.downloads_today(uid))
        await cq.answer(f"⛔ Daily limit {DAILY_LIMIT}/day — you have {left} left today.", show_alert=True)
        return False
    if sum(1 for j in JOBS.values() if j["uid"] == uid) >= MAX_PER_USER:
        await cq.answer("⏳ You already have a download running. Use /cancel to stop it.", show_alert=True)
        return False
    return True


# ---------------------------------------------------------- file helpers ----
_MAGIC = [(b"\x1aE\xdf\xa3", ".mkv"), (b"PK\x03\x04", ".zip"), (b"%PDF-", ".pdf"), (b"\xff\xd8\xff", ".jpg"),
          (b"\x89PNG\r\n\x1a\n", ".png"), (b"Rar!\x1a\x07", ".rar"), (b"7z\xbc\xaf\x27\x1c", ".7z"), (b"GIF8", ".gif"),
          (b"ID3", ".mp3")]
_TEXTY = (".html", ".htm", ".txt", ".json", ".xml", ".csv", ".srt", ".vtt", ".md")


def read_head(path: str, n: int = 64) -> bytes:
    try:
        with open(path, "rb") as fh:
            return fh.read(n)
    except OSError:
        return b""


def sniff_ext(head: bytes) -> str:
    if head[4:8] == b"ftyp":
        return ".mp4"
    for sig, ext in _MAGIC:
        if head.startswith(sig):
            return ext
    return ""


def validate_download(path: str, name: str, size: int, is_video: bool):
    """Raise if the 'file' is really an error page or absurdly small for a video."""
    head = read_head(path).lstrip().lower()
    if not name.lower().endswith(_TEXTY) and (head.startswith((b"<!doctype", b"<html", b"<?xml")) or head.startswith(b'{"')):
        raise RuntimeError("download link returned a web page / error instead of the file")
    if is_video and size < 50 * 1024:
        raise RuntimeError(f"downloaded video is only {human_size(size)} — link is probably broken")


async def _run(*cmd, timeout: int = 60):
    try:
        p = await asyncio.create_subprocess_exec(*cmd, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.DEVNULL)
        out, _ = await asyncio.wait_for(p.communicate(), timeout=timeout)
        return p.returncode, out
    except Exception:
        return 1, b""


async def probe_video(path: str):
    """(duration_s, width, height) — zeros if ffprobe is missing or fails."""
    if not shutil.which("ffprobe"):
        return 0, 0, 0
    rc, out = await _run("ffprobe", "-v", "error", "-select_streams", "v:0", "-show_entries",
                         "stream=width,height:format=duration", "-of", "json", path)
    try:
        d = json.loads(out)
        st = d["streams"][0]
        return int(float(d["format"]["duration"])), int(st.get("width") or 0), int(st.get("height") or 0)
    except Exception:
        return 0, 0, 0


async def make_thumb(path: str, out: str, duration: int) -> bool:
    """Cut a sharp frame from the video: longest side 320 px (Telegram's max), JPEG kept under 200 KB."""
    if not shutil.which("ffmpeg"):
        return False
    at = max(1, int(duration * 0.1)) if duration else 1
    vf = "scale='if(gt(iw,ih),320,-2)':'if(gt(iw,ih),-2,320)':flags=lanczos"
    for q in ("2", "5", "10"):  # best quality first, then smaller until it fits Telegram's 200 KB limit
        rc, _ = await _run("ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-ss", str(at), "-i", path, "-frames:v", "1",
                           "-vf", vf, "-q:v", q, out, timeout=60)
        if rc == 0 and os.path.exists(out) and 0 < os.path.getsize(out) <= 200 * 1024:
            return True
    return rc == 0 and os.path.exists(out) and 0 < os.path.getsize(out) <= 200 * 1024


async def _ffmpeg_segment(src: str, pattern: str, secs: int) -> list:
    p = await asyncio.create_subprocess_exec(
        "ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-i", src, "-c", "copy", "-map", "0",
        "-f", "segment", "-segment_time", str(secs), "-reset_timestamps", "1", pattern,
        stdout=asyncio.subprocess.DEVNULL, stderr=asyncio.subprocess.DEVNULL)
    await p.wait()
    return sorted(glob.glob(pattern.replace("%03d", "*")))


async def split_video(src: str, max_part: int) -> list:
    """Stream-copy split (no re-encode) into playable mp4 parts under max_part. [] if it cannot be done."""
    if not (shutil.which("ffmpeg") and shutil.which("ffprobe")):
        return []
    size = os.path.getsize(src)
    dur, _, _ = await probe_video(src)
    if dur <= 0:
        return []
    secs = max(30, int(max_part * 0.85 / (size / dur)))
    stem = os.path.splitext(src)[0]
    parts = await _ffmpeg_segment(src, f"{stem}_part%03d.mp4", secs)
    if not parts:
        return []
    fixed = []
    for part in parts:  # one corrective pass for parts that landed over budget (high-bitrate stretches)
        psize = os.path.getsize(part)
        if psize <= max_part:
            fixed.append(part)
            continue
        pdur, _, _ = await probe_video(part)
        ok = False
        for factor in (0.85, 0.6, 0.4, 0.25):  # cuts land on keyframes, so shrink the target until every piece fits
            if not pdur:
                break
            subs = await _ffmpeg_segment(part, part[:-4] + "_s%03d.mp4", max(3, int(pdur * (max_part / psize) * factor)))
            if subs and all(os.path.getsize(x) <= max_part for x in subs):
                os.remove(part)
                fixed.extend(subs)
                ok = True
                break
            for x in subs:
                os.remove(x)
        if not ok:
            return []  # could not get under the limit -> caller falls back to raw split
    return fixed


def split_bytes(src: str, max_part: int) -> list:
    """Raw split into name.ext.001, .002 … (join with `cat`, 7-Zip or HJSplit)."""
    parts, i = [], 1
    with open(src, "rb") as fh:
        while True:
            out = f"{src}.{i:03d}"
            written = 0
            with open(out, "wb") as o:
                while written < max_part:
                    chunk = fh.read(min(8 * 1024 * 1024, max_part - written))
                    if not chunk:
                        break
                    o.write(chunk)
                    written += len(chunk)
            if written == 0:
                os.remove(out)
                break
            parts.append(out)
            i += 1
    return parts


# ------------------------------------------------ fastest-link picker ----
PROBE_BYTES = 2 * 1024 * 1024   # speed test: first 2 MB (or 4 s) of each candidate link
PROBE_SECS = 4.0
RANK_FIRST = os.getenv("RANK_FIRST", "0") == "1"  # 1 = test all servers before downloading (slower start)
RANK_DEADLINE = 14.0            # never wait longer than this for resolvers + probes


async def probe_speed(url: str) -> float:
    """Bytes/s measured on the first PROBE_BYTES of `url` (0 = unusable). HLS playlists get a token score so they rank last."""
    if ".m3u8" in url.lower():
        return 1.0
    t0, got = time.time(), 0
    try:
        async with http.get(url, headers={**DL_HEADERS, "Range": f"bytes=0-{PROBE_BYTES - 1}"}, allow_redirects=True,
                            timeout=aiohttp.ClientTimeout(total=PROBE_SECS + 8, connect=8, sock_read=5)) as r:
            ctype = (r.headers.get("Content-Type") or "").lower()
            if r.status >= 400 or "text" in ctype or "json" in ctype:
                return 0.0
            async for chunk in r.content.iter_chunked(65536):
                got += len(chunk)
                if got >= PROBE_BYTES or time.time() - t0 >= PROBE_SECS:
                    break
    except Exception:
        return 0.0
    return got / max(time.time() - t0, 0.05) if got >= 32 * 1024 else 0.0


async def rank_links(f: tb.TeraFile) -> list:
    """All working download links for a single-file share, fastest first.
    The current link and every fallback resolver run at the same time; each link is speed-tested as soon as it resolves."""
    async def probe_file(name: str, cand: tb.TeraFile):
        if not cand or not cand.download_url:
            return None
        if f.size and cand.size and abs(cand.size - f.size) > 0.03 * f.size:  # not the same file
            return None
        return await probe_speed(cand.download_url), name, cand

    async def via_resolver(name: str, fn):
        res = await asyncio.wait_for(fn(f.source, http), timeout=RANK_DEADLINE)
        if len(res.files) != 1:
            return None
        return await probe_file(name, res.files[0])

    jobs = [asyncio.create_task(probe_file("current", f))]
    jobs += [asyncio.create_task(via_resolver(n, fn)) for n, fn in fallbacks.FALLBACKS]
    done, pending = await asyncio.wait(jobs, timeout=RANK_DEADLINE)
    for t in pending:
        t.cancel()
    found, seen = [], set()
    for t in done:
        try:
            r = t.result()
        except Exception:
            continue
        if r and r[0] > 0 and r[2].download_url not in seen:
            seen.add(r[2].download_url)
            found.append(r)
    found.sort(key=lambda x: -x[0])
    log.info("link ranking: %s", ", ".join(f"{n}={human_size(sp)}/s" for sp, n, _ in found) or "none usable")
    return [c for _, _, c in found]


async def alt_link(f: tb.TeraFile):
    """A fresh download link from the fallback resolvers (single-file shares only)."""
    if not f.source or f.fallback:
        return None
    from fallbacks import resolve_with_fallbacks
    try:
        res = await resolve_with_fallbacks(f.source, http)
        return res.files[0] if res.files and res.files[0].download_url else None
    except Exception as e:
        log.warning("alternate link failed: %s", e)
        return None


def build_caption(name: str, size: int, user, source_url: str = "", dl_s: float = 0, ul_s: float = None,
                  duration: int = 0, height: int = 0, part: str = "") -> str:
    """fbot-style upload caption (blockquote with file info, timings, downloaded-by, source, powered-by)."""
    def hms(sec):
        sec = max(0, int(round(sec)))
        h, r = divmod(sec, 3600)
        m, s_ = divmod(r, 60)
        return f"{h}:{m:02d}:{s_:02d}"

    by = f'<a href="tg://user?id={user.id}">{html.escape(smallcaps(user.first_name or "User"))}</a>'
    src_txt = html.escape(smallcaps("Terabox Link"))
    src = f'<a href="{html.escape(source_url, quote=True)}">{src_txt}</a>' if source_url.startswith("http") else src_txt
    powered = f'<a href="{html.escape(POWERED_BY_URL, quote=True)}">{html.escape(smallcaps(POWERED_BY))}</a>'
    lines = [f"📄 {smallcaps('File Name')}: {html.escape(smallcaps(name[:120]))}"]
    lines.append(f"📦 {smallcaps('Size')}: {human_size(size)}")
    if part:
        lines.append(f"🧩 {smallcaps('Part')}: {part}")
    if height:
        lines.append(f"🎞️ {smallcaps('Quality')}: {height}p")
    if duration:
        lines.append(f"⏱️ {smallcaps('Duration')}: {hms(duration)}")
    lines.append(f"⬇️ {smallcaps('Downloaded in')}: {hms(dl_s)} sec")
    if ul_s is not None:
        lines.append(f"⬆️ {smallcaps('Uploaded in')}: {hms(ul_s)} sec")
    lines.append(f"🙋 {smallcaps('Downloaded by')}: {by}")
    lines.append(f"🔗 {smallcaps('Source')}: {src}")
    return f"<blockquote>{chr(10).join(lines)}</blockquote>\n\n⚡ {smallcaps('Powered by')} {powered}"


async def transfer(client: Client, msg, user, f: tb.TeraFile, job: dict, label: str = "", kb=None, back_kb=None, source_url: str = "") -> str:
    """Download `f` and upload it to msg.chat. Edits `msg` with progress.
    Returns "ok", "cancelled", "toobig" or "failed". Never raises."""
    uid = user.id
    cancel_kb = kb
    workdir = os.path.join(DOWNLOAD_DIR, f"{uid}_{uuid.uuid4().hex[:8]}")
    os.makedirs(workdir, exist_ok=True)
    path = os.path.join(workdir, safe_name(f.name))
    title = f"{label}<b>{html.escape(f.name[:80])}</b>"
    lbl_line = f"┣⪼ 📦 Item: {label.strip()}\n" if label else ""
    t0 = time.time()
    try:
        if not f.download_url:
            raise RuntimeError("no download link for this file")
        if f.size and f.size > MAX_DL_BYTES:
            raise RuntimeError("TOO_BIG")
        await safe_edit(msg, f"⏳ <b>Queued…</b>\n{title}", cancel_kb)
        async with sem:
            if job["cancel"]:
                raise Cancelled()

            async def dl_prog(done, total):
                await safe_edit(msg, progress_text("download", f.name, done, total, t0, lbl_line), cancel_kb)

            cur, tried_alt, size, rest = f, False, 0, []
            # progress bar starts straight away (no "Finding the fastest server" step);
            # a slow / broken link is still swapped automatically by the slow-link check below
            await safe_edit(msg, progress_text("download", f.name, 0, f.size or 0, t0, lbl_line), cancel_kb)
            if f.source and RANK_FIRST:  # optional: speed-test all servers BEFORE starting (RANK_FIRST=1)
                await safe_edit(msg, f"⚡ <b>Finding the fastest server…</b>\n{title}", cancel_kb)
                ranked = await rank_links(f)
                if ranked:
                    cur, rest = ranked[0], ranked[1:]
            d0 = time.time()
            while True:
                job["slow_check"] = bool(rest or (cur.source and not cur.fallback and not tried_alt))  # only if an alternative exists
                try:
                    size = await download_file(cur.download_url, path, job, cur.size, dl_prog)
                    validate_download(path, f.name, size, f.is_video)
                    break
                except (Cancelled, asyncio.CancelledError):
                    raise
                except Exception as e:
                    if str(e) == "TOO_BIG" or (not rest and (tried_alt or not (cur.source and not cur.fallback))):
                        raise
                    log.warning("download failed (%s) — trying another link", e)
                    await safe_edit(msg, f"🔁 <b>Link problem — switching to another server…</b>\n{title}", cancel_kb)
                    if rest:
                        cur = rest.pop(0)
                        continue
                    tried_alt = True
                    alt = await alt_link(cur)
                    if not alt:
                        raise
                    cur = alt
            job["slow_check"] = False
            dl_secs = time.time() - d0

            # fix a missing extension from the file's magic bytes
            if "." not in os.path.basename(path):
                ext = sniff_ext(read_head(path))
                if ext:
                    os.replace(path, path + ext)
                    path += ext

            thumb_path = None
            if f.thumb and f.thumb.startswith("http"):
                try:
                    async with http.get(f.thumb, timeout=aiohttp.ClientTimeout(total=15)) as tr:
                        if tr.status == 200 and "image" in (tr.headers.get("Content-Type") or ""):
                            thumb_path = os.path.join(workdir, "thumb.jpg")
                            with open(thumb_path, "wb") as th:
                                th.write(await tr.read())
                except Exception:
                    thumb_path = None

            # split files that exceed Telegram's per-file limit
            parts = [path]
            if size > MAX_FILE_BYTES:
                await safe_edit(msg, f"✂️ <b>Splitting into parts…</b>\n{title}", cancel_kb)
                parts = (await split_video(path, MAX_FILE_BYTES)) if f.is_video else []
                if parts:
                    os.remove(path)
                else:
                    parts = await asyncio.to_thread(split_bytes, path, MAX_FILE_BYTES)
                    os.remove(path)
                    split_note = "\n🧩 Raw split — join the parts (<code>cat name.* &gt; name</code> or 7-Zip) to get the file."
                    await client.send_message(msg.chat.id, f"ℹ️ <b>{html.escape(f.name[:80])}</b> was too large and is sent as {len(parts)} raw parts." + split_note, parse_mode=ParseMode.HTML)

            for pi, part in enumerate(parts, 1):
                if job["cancel"]:
                    raise Cancelled()
                is_vid = f.is_video and part.lower().endswith((".mp4", ".mkv", ".mov", ".webm", ".m4v", ".avi", ".ts", ".flv", ".3gp"))
                duration = width = height = 0
                pthumb = thumb_path if len(parts) == 1 else None
                if is_vid:
                    duration, width, height = await probe_video(part)
                    tp = os.path.join(workdir, f"frame_{pi}.jpg")
                    if await make_thumb(part, tp, duration):  # sharp frame from the video beats the small Terabox thumbnail
                        pthumb = tp
                psize = os.path.getsize(part)
                plabel = f" • part {pi}/{len(parts)}" if len(parts) > 1 else ""
                ptitle = title + plabel
                u0, ulast = time.time(), [0.0]

                async def up_prog(cur_b, total_b, part_name=part, u0=u0, ulast=ulast):
                    if job["cancel"]:
                        client.stop_transmission()
                    now = time.time()
                    if now - ulast[0] < EDIT_INTERVAL:
                        return
                    ulast[0] = now
                    await safe_edit(msg, progress_text("upload", f.name if len(parts) == 1 else os.path.basename(part_name), cur_b, total_b, u0, lbl_line), cancel_kb)

                shown = os.path.basename(part) if len(parts) > 1 else f.name
                cap_kw = dict(name=shown, size=psize, user=user, source_url=source_url or f.source or "", dl_s=dl_secs,
                              duration=duration, height=height, part=f"{pi}/{len(parts)}" if len(parts) > 1 else "")
                caption = build_caption(**cap_kw)
                try:
                    u_start = time.time()
                    if is_vid:
                        sent = await client.send_video(msg.chat.id, part, caption=caption, parse_mode=ParseMode.HTML, supports_streaming=True,
                                                       duration=duration, width=width, height=height, thumb=pthumb, progress=up_prog)
                    else:
                        sent = await client.send_document(msg.chat.id, part, caption=caption, parse_mode=ParseMode.HTML,
                                                          thumb=pthumb, progress=up_prog)
                    try:  # add the real upload time now that it is known
                        await sent.edit_caption(build_caption(**cap_kw, ul_s=time.time() - u_start), parse_mode=ParseMode.HTML)
                    except Exception:
                        pass
                except Exception:
                    if job["cancel"]:
                        raise Cancelled()
                    raise

        store.record_download(uid, size)
        try:  # video/file is already in the chat -> remove the progress/menu message instead of leaving a "Done!" card
            await msg.delete()
        except Exception:
            await safe_edit(msg, f"✅ <b>Done!</b>\n{title}\n⏱ {human_time(time.time() - t0)}")
        await log_to_channel(client, f"✅ <b>Download</b>\n{user_tag(user)}\n📄 {html.escape(f.name[:100])}\n💾 {human_size(size)}")
        return "ok"
    except Cancelled:
        await safe_edit(msg, f"🛑 <b>Cancelled</b>\n{title}", back_kb)
        return "cancelled"
    except Exception as e:
        if str(e) == "TOO_BIG":
            await safe_edit(msg, f"⚠️ <b>Too big</b> (limit {human_size(MAX_DL_BYTES)})\n{title}\nUse the Stream / Direct link buttons.", back_kb)
            return "toobig"
        log.exception("download failed")
        await safe_edit(msg, f"❌ <b>Failed</b>\n{title}\n<code>{html.escape(str(e)[:200])}</code>", back_kb)
        await log_to_channel(client, f"❌ <b>Download failed</b>\n{user_tag(user)}\n<code>{html.escape(str(e)[:300])}</code>")
        return "failed"
    finally:
        shutil.rmtree(workdir, ignore_errors=True)


async def on_direct_link(client: Client, cq):
    """Show API being called for Direct Link and open the URL."""
    _, rid, idx = cq.data.split(":")
    uid = cq.from_user.id
    f = _get_file(rid, int(idx), uid)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    
    # Show API is being called
    await safe_edit(cq.message, SC("🔄 <b>Calling API to get direct link…</b>"))
    
    # Get fresh direct URL from current file data
    if not f.download_url:
        log.warning("❌ No direct link in file data for %s", f.name[:50])
        await safe_edit(cq.message, SC("❌ <b>No direct link available</b>\n\n<code>API Error: File has no download_url</code>"))
        return
    
    link = clean_url(f.download_url, allow_m3u8=False)
    if not link:
        log.warning("❌ Could not clean direct link for %s", f.name[:50])
        await safe_edit(cq.message, SC("❌ <b>Invalid direct link</b>"))
        return
    
    log.info("✅ Direct Link API: %s", link[:100])
    
    # Show the link and open button
    text = SC(f"<b>Direct Link Ready</b>\n\nName: <code>{html.escape(f.name)}</code>\nSize: <code>{human_size(f.size)}</code>")
    kb = Markup([[mbtn("🔗 Open Link", url=link, style=BTN_PRIMARY)]])
    await safe_edit(cq.message, text, kb)
    await cq.answer("✅ Direct link ready!")


async def on_download(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    uid = cq.from_user.id
    f = _get_file(rid, int(idx), uid)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    if not await precheck(client, cq):
        return
    
    # Show API is being called
    await safe_edit(cq.message, SC("🔄 <b>Calling API to get download link…</b>"))
    
    # Get fresh download URL from current file data
    if not f.download_url:
        log.warning("❌ No download link in file data for %s", f.name[:50])
        await safe_edit(cq.message, SC("❌ <b>No download link available</b>\n\n<code>API Error: File has no download_url</code>"))
        await cq.answer("No download link available for this file.", show_alert=True)
        return
    
    log.info("✅ Download API link ready: %s", f.download_url[:100])
    
    if f.size and f.size > MAX_DL_BYTES:
        await cq.answer(f"File is bigger than {human_size(MAX_DL_BYTES)} — use the Direct Link / Stream button.", show_alert=True)
        return
    
    await cq.answer("⬇️ Starting…")
    jid = uuid.uuid4().hex[:8]
    job = {"cancel": False, "uid": uid}
    JOBS[jid] = job
    try:
        multi = len(RESULTS[rid]["res"].files) > 1
        await transfer(client, cq.message, cq.from_user, f, job,
                       source_url=RESULTS.get(rid, {}).get("url", ""),
                       kb=Markup([[Btn("🛑 Cancel", callback_data=f"x:{jid}")]]),
                       back_kb=file_menu_kb(rid, int(idx), f, back=multi))
    finally:
        JOBS.pop(jid, None)


async def on_download_all(client: Client, cq):
    """Send every file of a folder one after another. One status message per file; Cancel stops the whole batch."""
    rid = cq.data.split(":")[1]
    uid = cq.from_user.id
    ent = RESULTS.get(rid)
    if not ent or ent["exp"] < time.time() or (ent["uid"] != uid and not is_admin(uid)):
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    files = [f for f in ent["res"].files if f.download_url and not f.is_dir]
    if DAILY_LIMIT and not is_unlimited(uid):
        files = files[:max(0, DAILY_LIMIT - store.downloads_today(uid))] or files
    if not await precheck(client, cq, need=min(len(files), 1)):
        return
    await cq.answer(f"⬇️ Sending {len(files)} files one by one…")
    jid = uuid.uuid4().hex[:8]
    job = {"cancel": False, "uid": uid}
    JOBS[jid] = job
    kb = Markup([[Btn("🛑 Cancel all", callback_data=f"x:{jid}")]])
    chat_id = cq.message.chat.id
    ok = bad = skipped = 0
    try:
        for i, f in enumerate(files, 1):
            if job["cancel"]:
                break
            if DAILY_LIMIT and not is_unlimited(uid) and store.downloads_today(uid) >= DAILY_LIMIT:
                await client.send_message(chat_id, f"⛔ Daily limit reached ({DAILY_LIMIT}/day). Stopped at {i - 1}/{len(files)}.")
                break
            m = await client.send_message(chat_id, f"⏳ <b>[{i}/{len(files)}]</b> {html.escape(f.name[:60])}", parse_mode=ParseMode.HTML)
            r = await transfer(client, m, cq.from_user, f, job, label=f"[{i}/{len(files)}] ", kb=kb, source_url=ent.get("url", ""))
            if r == "ok":
                ok += 1
            elif r in ("toobig",):
                skipped += 1
            elif r == "failed":
                bad += 1
            else:
                break
        summary = f"📦 <b>Folder finished</b>\n✅ Sent: <b>{ok}</b>"
        if bad:
            summary += f"\n❌ Failed: <b>{bad}</b>"
        if skipped:
            summary += f"\n⚠️ Too big (use links): <b>{skipped}</b>"
        if job["cancel"]:
            summary = "🛑 <b>Batch cancelled</b>\n" + summary.split("\n", 1)[1]
        await client.send_message(chat_id, summary, parse_mode=ParseMode.HTML)
    finally:
        JOBS.pop(jid, None)


# ---------------------------------------------------------------- admin ----
async def on_stats(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    d = store.data
    await message.reply_text(
        SC("📊 <b>Bot Stats</b>\n\n"
           f"👥 <b>Total Users:</b> {len(d['users'])}\n"
           f"💎 <b>Premium Users:</b> {len(store.premium_ids())}\n"
           f"🚫 <b>Banned Users:</b> {store.banned_count()}\n\n"
           f"📦 <b>Total Downloads:</b> {d['total_downloads']}\n"
           f"💾 <b>Data Sent:</b> {human_size(d['total_bytes'])}\n"
           f"🔄 <b>Active Jobs:</b> {len(JOBS)}\n"
           f"⏱ <b>Uptime:</b> {human_time(time.time() - START_TIME)}"),
        parse_mode=ParseMode.HTML)


_ID_IN_TEXT = re.compile(r"(?:user\s*id|id|user)\s*[:=\-]?\s*(\d{5,15})", re.I)


async def target_user_id(client: Client, message):
    """Who to ban/unban: /ban <id>, /ban @username, or a reply to the user's message / a forwarded message /
    a bot notice that contains 'ID: 123456789' (e.g. the New User log). Returns int or None."""
    if len(message.command) >= 2:
        arg = message.command[1].strip()
        if re.fullmatch(r"-?\d+", arg):
            return int(arg)
        if arg.startswith("@") or re.fullmatch(r"[A-Za-z]\w{4,}", arg):
            try:
                return (await client.get_users(arg.lstrip("@"))).id
            except Exception:
                return None
        return None
    r = message.reply_to_message
    if not r:
        return None
    fwd = getattr(r, "forward_from", None) or getattr(getattr(r, "forward_origin", None), "sender_user", None)
    if fwd and not getattr(fwd, "is_bot", False):
        return fwd.id
    fu = r.from_user
    if fu and not fu.is_bot and not is_admin(fu.id):
        return fu.id
    m = _ID_IN_TEXT.search(r.text or r.caption or "")
    return int(m.group(1)) if m else None


async def on_ban(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    on = message.command[0].lower() == "ban"
    cmd = "ban" if on else "unban"
    uid = await target_user_id(client, message)
    if uid is None:
        await message.reply_text(SC(f"⚠️ <b>Usage:</b> <code>/{cmd} &lt;user_id&gt;</code>\nYa kisi user ke message par reply karke <code>/{cmd}</code> likho."), parse_mode=ParseMode.HTML)
        return
    if on and is_admin(uid):
        await message.reply_text(SC("⚠️ Can't ban an admin."))
        return
    store.ban(uid, on)
    await store.flush()
    await message.reply_text(SC(f"{'🚫' if on else '✅'} <code>{uid}</code> has been {'banned' if on else 'unbanned'}."), parse_mode=ParseMode.HTML)
    try:
        await client.send_message(uid, SC("🚫 You've been banned from using this bot." if on
                                          else "✅ You've been unbanned — you can use the bot again."))
    except Exception as e:
        log.warning("Couldn't notify %s about %s: %s", uid, cmd, e)


async def on_addpremium(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    usage = ("Usage:\n<code>/addpremium &lt;user_id&gt; &lt;days&gt;</code>\n"
             "<code>/addpremium &lt;user_id&gt; lifetime</code>\n\nExample: <code>/addpremium 123456789 30</code>")
    if len(parts) < 3 or not parts[1].isdigit():
        await message.reply_text(usage, parse_mode=ParseMode.HTML)
        return
    uid, arg = int(parts[1]), parts[2].lower()
    if arg in ("lifetime", "life", "forever", "0"):
        days = 0
    elif arg.isdigit():
        days = int(arg)
    else:
        await message.reply_text(usage, parse_mode=ParseMode.HTML)
        return
    info = store.add_premium(uid, days)
    await store.flush()
    plan = "Lifetime ♾️" if info["lifetime"] else f"{info['days_left']} day(s) left"
    await message.reply_text(f"✅ <b>Premium added</b>\n\nUser: <code>{uid}</code>\nPlan: <code>{plan}</code>", parse_mode=ParseMode.HTML)
    try:
        await client.send_message(uid, SC(f"🎉 <b>Premium Activated!</b>\n\n💎 Plan: <code>{plan}</code>\n✨ Ab aapko unlimited downloads milenge. Enjoy!"),
                                  parse_mode=ParseMode.HTML)
    except Exception:
        await message.reply_text("ℹ️ User ko notify nahi kar paya (bot start nahi kiya ya block hai).")
    await log_to_channel(client, f"💎 <b>Premium added</b> by <code>{message.from_user.id}</code>\nUser: <code>{uid}</code> · {plan}")


async def on_removepremium(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].isdigit():
        await message.reply_text("Usage: <code>/removepremium &lt;user_id&gt;</code>", parse_mode=ParseMode.HTML)
        return
    uid = int(parts[1])
    had = store.remove_premium(uid)
    await store.flush()
    if not had:
        await message.reply_text(f"⚠️ <code>{uid}</code> ke paas premium nahi tha.", parse_mode=ParseMode.HTML)
        return
    await message.reply_text(f"🗑 <b>Premium removed</b> for <code>{uid}</code>", parse_mode=ParseMode.HTML)
    try:
        await client.send_message(uid, SC("⚠️ <b>Aapka Premium plan hata diya gaya hai.</b>\n\n💎 Dobara lene ke liye /plans dekho."), parse_mode=ParseMode.HTML)
    except Exception:
        pass
    await log_to_channel(client, f"🗑 <b>Premium removed</b> by <code>{message.from_user.id}</code>\nUser: <code>{uid}</code>")


async def on_premiumlist(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    ids = store.premium_ids()
    if not ids:
        await message.reply_text("No premium users yet.")
        return
    rows = []
    for i in ids[:60]:
        pi = store.premium_info(i)
        rows.append(f"• <code>{i}</code> — " + ("Lifetime ♾️" if pi["lifetime"] else f"{pi['days_left']}d left"))
    await message.reply_text(f"💎 <b>Premium users ({len(ids)})</b>\n\n" + "\n".join(rows), parse_mode=ParseMode.HTML)


async def _broadcast_one(client: Client, cid: int, text, src, from_chat_id: int) -> str:
    try:
        if text is not None:
            await client.send_message(cid, text)
        else:
            await client.copy_message(chat_id=cid, from_chat_id=from_chat_id, message_id=src.id)
        return "success"
    except FloodWait as e:
        await asyncio.sleep(e.value + 1)
        return await _broadcast_one(client, cid, text, src, from_chat_id)
    except (InputUserDeactivated, UserIsBlocked, PeerIdInvalid):
        store.remove_user(cid)
        return "removed"
    except Exception as e:
        log.warning("broadcast failed for %s: %s", cid, e)
        return "failed"


async def on_broadcast(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    src = message.reply_to_message
    text = None
    if len(message.command) >= 2:
        text = message.text.split(None, 1)[1]
    elif not src:
        await message.reply_text(
            SC("⚠️ <b>Usage:</b> <code>/broadcast &lt;message&gt;</code>\n"
               "(or reply to a message with just <code>/broadcast</code> to forward that)"),
            parse_mode=ParseMode.HTML)
        return
    ids = store.user_ids()
    total = len(ids)
    status = await message.reply_text(SC(f"📣 Broadcasting to {total} users..."))
    done = success = removed = failed = 0
    for cid in ids:
        r = await _broadcast_one(client, cid, text, src, message.chat.id)
        if r == "success":
            success += 1
        elif r == "removed":
            removed += 1
        else:
            failed += 1
        done += 1
        if done % 20 == 0 or done == total:
            try:
                await status.edit_text(
                    SC("📣 <b>Broadcast in progress...</b>\n\n"
                       f"👥 Total: {total}\n💫 Done: {done}/{total}\n✅ Success: {success}\n"
                       f"🚫 Removed (blocked/deleted): {removed}\n❌ Failed: {failed}"),
                    parse_mode=ParseMode.HTML)
            except Exception:
                pass
        await asyncio.sleep(0.05)
    await store.flush()
    try:
        await status.edit_text(
            SC("📣 <b>Broadcast done.</b>\n\n"
               f"✅ Success: {success}\n🚫 Removed (blocked/deleted): {removed}\n❌ Failed: {failed}"),
            parse_mode=ParseMode.HTML)
    except Exception:
        pass


async def on_users(client: Client, message):
    """Export all users as a JSON file (same as fbot's /users)."""
    if not is_admin(message.from_user.id):
        return
    export = [{"id": int(k), "name": v.get("name", ""), "is_banned": int(k) in store.data["banned"],
               "is_premium": store.is_premium(int(k)), "downloads": v.get("dl", 0), "first_seen": v.get("joined")}
              for k, v in store.data["users"].items()]
    path = f"/tmp/terabox_users_{message.chat.id}.json"
    try:
        with open(path, "w", encoding="utf-8") as f:
            json.dump(export, f, indent=2, ensure_ascii=False)
        await message.reply_document(path, caption=SC(f"📄 {len(export)} users exported."))
    except Exception as e:
        await message.reply_text(SC(f"⚠️ Error exporting users: {e}"))
    finally:
        try:
            os.remove(path)
        except Exception:
            pass


def status_text(started: bool, username: str) -> str:
    """fbot-style start / stop notice for the log channel."""
    ist = time.strftime("%I:%M %p IST", time.gmtime(time.time() + 5.5 * 3600))
    head = "🚀 <b>Bot successfully started!</b>" if started else "🛑 <b>Bot stopped!</b>"
    return (f"{head}\n\n"
            f"⭐ Bot: @{username}\n"
            f"👥 Users: {len(store.data['users'])}\n"
            f"⏳ Time: {ist}\n\n"
            f"👑 Developed by {DEVELOPER_URL.replace('https://t.me/', '@')}")


BOT_COMMANDS_LIST = [
    BotCommand("start", "🚀 Start the bot"),
    BotCommand("help", "❓ How to use the bot"),
    BotCommand("about", "ℹ️ About this bot"),
    BotCommand("plans", "💎 Premium plans"),
    BotCommand("myplan", "📊 Your status"),
    BotCommand("premium", "💎 Premium plans (same as /plans)"),
    BotCommand("mystatus", "📊 Your status (same as /myplan)"),
    BotCommand("cancel", "🚫 Cancel current active download"),
    BotCommand("stats", "📊 [Admin] Bot statistics"),
    BotCommand("broadcast", "📣 [Admin] Broadcast a message (reply to it)"),
    BotCommand("addpremium", "💎 [Admin] Add premium (/addpremium <id> <days|lifetime>)"),
    BotCommand("removepremium", "🗑 [Admin] Remove premium (/removepremium <id>)"),
    BotCommand("premiumlist", "📋 [Admin] List premium users"),
    BotCommand("users", "👥 [Admin] Export users list"),
    BotCommand("ban", "⛔ [Admin] Ban a user (/ban <id> or reply)"),
    BotCommand("unban", "✅ [Admin] Unban a user (/unban <id> or reply)"),
]


# ------------------------------------------------------------------ main ----
async def main():
    global http
    os.makedirs(DOWNLOAD_DIR, exist_ok=True)
    http = aiohttp.ClientSession()
    web_runner = await stream_proxy.start_web_server(PORT, lambda: http, DL_HEADERS)
    pinger = asyncio.create_task(stream_proxy.keep_alive_loop(lambda: http))
    app = Client("terabox_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, in_memory=True)

    private = filters.private
    app.add_handler(MessageHandler(on_start, filters.command("start") & private))
    app.add_handler(MessageHandler(on_help, (filters.command("help") | menu_text_filter(BTN_HELP)) & private))
    app.add_handler(MessageHandler(on_plans, (filters.command(["plans", "premium"]) | menu_text_filter(BTN_PLANS)) & private))
    app.add_handler(MessageHandler(on_my_status, (filters.command(["myplan", "mystatus"]) | menu_text_filter(BTN_MYSTATUS)) & private))
    app.add_handler(MessageHandler(on_support, menu_text_filter(BTN_SUPPORT) & private))
    app.add_handler(CallbackQueryHandler(on_fallback_download, filters.regex(r"^fallback_download$")))
    app.add_handler(CallbackQueryHandler(on_fallback_status, filters.regex(r"^fallback_status$")))
    app.add_handler(CallbackQueryHandler(on_show_plans, filters.regex(r"^show_plans$")))
    app.add_handler(CallbackQueryHandler(on_plans_back, filters.regex(r"^plans_back$")))
    app.add_handler(MessageHandler(on_about, filters.command("about") & private))
    app.add_handler(CallbackQueryHandler(on_about_close, filters.regex(r"^about_close$")))
    app.add_handler(MessageHandler(on_cancel, filters.command("cancel") & private))
    app.add_handler(MessageHandler(on_stats, filters.command("stats") & private))
    app.add_handler(MessageHandler(on_ban, filters.command(["ban", "unban"]) & (private | filters.group)))
    app.add_handler(MessageHandler(on_broadcast, filters.command("broadcast") & private))
    app.add_handler(MessageHandler(on_users, filters.command("users") & private))
    app.add_handler(MessageHandler(on_addpremium, filters.command("addpremium") & private))
    app.add_handler(MessageHandler(on_removepremium, filters.command(["removepremium", "rmpremium"]) & private))
    app.add_handler(MessageHandler(on_premiumlist, filters.command("premiumlist") & private))
    app.add_handler(MessageHandler(on_link, filters.text & private & NOT_MENU_BUTTON & ~filters.command(["start", "help", "about", "cancel", "stats", "ban", "unban", "broadcast", "plans", "premium", "myplan", "mystatus", "addpremium", "removepremium", "rmpremium", "premiumlist", "users"])))
    app.add_handler(CallbackQueryHandler(on_verify, filters.regex(r"^verify$")))
    app.add_handler(CallbackQueryHandler(on_file_pick, filters.regex(r"^f:")))
    app.add_handler(CallbackQueryHandler(on_download, filters.regex(r"^dl:")))
    app.add_handler(CallbackQueryHandler(on_stream, filters.regex(r"^st:")))
    app.add_handler(CallbackQueryHandler(on_direct_link, filters.regex(r"^dr:")))
    app.add_handler(CallbackQueryHandler(on_menu_back, filters.regex(r"^m:")))
    app.add_handler(CallbackQueryHandler(on_cancel_menu, filters.regex(r"^cx:")))
    app.add_handler(CallbackQueryHandler(on_download_all, filters.regex(r"^all:")))
    app.add_handler(CallbackQueryHandler(on_page, filters.regex(r"^p:")))
    app.add_handler(CallbackQueryHandler(on_cancel_btn, filters.regex(r"^x:")))

    await app.start()
    try:  # "/" command menu (same idea as fbot's BOT_COMMANDS_LIST) - no need to set it in BotFather
        await app.set_bot_commands(BOT_COMMANDS_LIST)
    except Exception as e:
        log.warning("set_bot_commands failed: %s", e)
    me = await app.get_me()
    log.info("Started as @%s", me.username)
    saver = asyncio.create_task(store.autosave_loop())
    await log_to_channel(app, status_text(True, me.username))
    try:
        await idle()
    finally:
        saver.cancel()
        pinger.cancel()
        await store.flush()
        await log_to_channel(app, status_text(False, me.username))
        await app.stop()
        await http.close()
        await web_runner.cleanup()


if __name__ == "__main__":
    asyncio.run(main())
