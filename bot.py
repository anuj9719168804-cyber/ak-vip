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
import time
import uuid

import aiohttp
from dotenv import load_dotenv
from pyrogram import Client, filters, idle
from pyrogram.enums import ChatMemberStatus, ParseMode
from pyrogram.errors import FloodWait, MessageNotModified, UserNotParticipant
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.types import InlineKeyboardButton as Btn, InlineKeyboardMarkup as Markup, LinkPreviewOptions, BotCommand
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


def is_admin(uid: int) -> bool:
    return uid in ADMINS


async def safe_edit(msg, text: str, markup=None):
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
        log.debug("edit failed: %s", e)


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
    if START_PHOTO_URL and _START_PHOTO_OK:
        try:
            m = re.fullmatch(r"https?://t\.me/([A-Za-z0-9_]+)/(\d+)", START_PHOTO_URL)
            if m:  # t.me post link is not an image url -> copy that post (photo) with our caption
                coro = client.copy_message(message.chat.id, m.group(1), int(m.group(2)),
                                           caption=caption, parse_mode=ParseMode.HTML)
            else:
                coro = message.reply_photo(START_PHOTO_URL, caption=caption, parse_mode=ParseMode.HTML)
            await asyncio.wait_for(coro, timeout=12)
            return
        except Exception as e:
            _START_PHOTO_OK = False  # don't make every /start wait on a broken photo
            log.warning("start photo failed (disabled until restart), sending text: %s", e)
    await message.reply_text(caption, parse_mode=ParseMode.HTML, link_preview_options=NO_PREVIEW)


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
    """fbot-style 'Choose an action' keyboard: Download / Stream Link / Cancel (or Back to list)."""
    rows = [[mbtn("🔽 Download", f"dl:{rid}:{idx}", style=BTN_PRIMARY)],
            [mbtn("🔗 Stream Link", f"st:{rid}:{idx}", style=BTN_PRIMARY)]]
    if back:
        rows.append([mbtn("⬅️ Back to list", f"p:{rid}:{idx // PAGE_SIZE}", style=BTN_DANGER)])
    else:
        rows.append([mbtn("❌ Cancel", f"cx:{rid}", style=BTN_DANGER)])
    return Markup(rows)


def stream_link_for(f: tb.TeraFile) -> str:
    """Playable link for the Stream button: our own /stream proxy for plain files (seekable), else the raw stream url."""
    src = next((u for u in (f.stream_url, f.download_url) if u and u.startswith("http") and ".m3u8" not in u.lower()), None)
    if src:
        u = stream_proxy.register_stream(src, f.name, f.size)
        if u:
            return u
    return f.stream_url or f.m3u8_url or ""


def stream_card(rid: str, idx: int, f: tb.TeraFile, back: bool):
    su = stream_link_for(f)
    rows = []
    if su and su.startswith("http") and len(su) < 2000:
        rows.append([mbtn("▶️ Stream / Play", url=su, style=BTN_PRIMARY)])
    more = []
    for label, u in (("🔗 Direct Link", f.download_url), ("📺 M3U8", f.m3u8_url)):
        if u and u.startswith("http") and len(u) < 2000 and u != su:
            more.append(mbtn(label, url=u))
    if more:
        rows.append(more)
    rows.append([mbtn("⬅️ Back", f"f:{rid}:{idx}" if back else f"m:{rid}:{idx}", style=BTN_DANGER)])
    if not su:
        return SC("<b>No stream link found for this file.</b>"), Markup(rows)
    return SC(f"<b>Stream Link Ready</b>\n\nName: <code>{html.escape(f.name)}</code>\nSize: <code>{human_size(f.size)}</code>"), Markup(rows)


_CATS = {"Video": {"mp4", "mkv", "mov", "avi", "webm", "m4v", "ts", "flv", "3gp"},
         "Audio": {"mp3", "m4a", "aac", "ogg", "flac", "wav"},
         "Image": {"jpg", "jpeg", "png", "gif", "webp"},
         "Archive": {"zip", "rar", "7z", "tar", "gz"},
         "Document": {"pdf", "doc", "docx", "txt", "xls", "xlsx", "ppt", "pptx", "epub"},
         "App": {"apk", "exe", "msi"}}


def file_category(name: str) -> str:
    ext = name.lower().rsplit(".", 1)[-1] if "." in name else ""
    return next((c for c, exts in _CATS.items() if ext in exts), "File")


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
        async with http.get(url, headers=DL_HEADERS, timeout=aiohttp.ClientTimeout(total=12)) as r:
            if r.status != 200 or "image" not in (r.headers.get("Content-Type") or ""):
                return None
            data = await r.content.read(5 * 1024 * 1024)
        bio = io.BytesIO(data)
        bio.name = "thumb.jpg"
        return bio if len(data) > 500 else None
    except Exception:
        return None


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
        res = await tb.fetch_terabox(url, session=http)
    except Exception as e:
        log.warning("fetch failed for %s: %s", url, e)
        await safe_edit(status, "❌ <b>Could not fetch this link.</b>\nIt may be invalid, private or deleted. Try again later.")
        await log_to_channel(client, f"⚠️ <b>Fetch failed</b>\n{user_tag(u)}\n<code>{html.escape(url)}</code>\n<code>{html.escape(str(e)[:300])}</code>")
        return

    skipped = 0
    if any(f.is_dir for f in res.files):  # folder share: walk the tree and flatten to a file list
        await safe_edit(status, "📂 <b>Folder detected — scanning files…</b>")
        files, skipped = await tb.list_folder(url, res, session=http, max_files=MAX_FOLDER_FILES)
        if not files:
            await safe_edit(status, "📂 This folder has no downloadable files.")
            return
        res = tb.TeraResult(title=res.title, files=files)

    if len(res.files) == 1 and not skipped and not res.files[0].is_dir and not res.files[0].fallback:
        res.files[0].source = url.split("?")[0]  # single-file share: allows switching to another server on a bad link
    gc_results()
    rid = uuid.uuid4().hex[:8]
    RESULTS[rid] = {"res": res, "uid": u.id, "exp": time.time() + 3600, "skipped": skipped, "url": url}
    if len(res.files) == 1:
        f = res.files[0]
        text, kb = menu_text(url, f), file_menu_kb(rid, 0, f)
        thumb = await fetch_thumb(f.thumb)
        if thumb:
            try:
                await message.reply_photo(thumb, caption=text[:1024], parse_mode=ParseMode.HTML, reply_markup=kb)
                await status.delete()
                return
            except Exception as e:
                log.warning("thumbnail menu failed, sending text: %s", e)
        await safe_edit(status, text, kb)
    else:
        text, kb = folder_page(rid, 0)
        await safe_edit(status, text, kb)


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
    await safe_edit(cq.message, file_card(f), file_menu_kb(rid, int(idx), f, back=multi))


async def on_stream(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    f = _get_file(rid, int(idx), cq.from_user.id)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    await cq.answer()
    multi = len(RESULTS[rid]["res"].files) > 1
    text, kb = stream_card(rid, int(idx), f, back=multi)
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
    if DAILY_LIMIT and not is_admin(uid) and store.downloads_today(uid) + need > DAILY_LIMIT:
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
    if not shutil.which("ffmpeg"):
        return False
    at = max(1, int(duration * 0.1)) if duration else 1
    rc, _ = await _run("ffmpeg", "-nostdin", "-loglevel", "error", "-y", "-ss", str(at), "-i", path, "-frames:v", "1",
                       "-vf", "scale=320:-2", out, timeout=60)
    return rc == 0 and os.path.exists(out) and os.path.getsize(out) > 0


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

            cur, tried_alt, size = f, False, 0
            d0 = time.time()
            while True:
                job["slow_check"] = bool(cur.source and not cur.fallback and not tried_alt)  # only if an alternative exists
                try:
                    size = await download_file(cur.download_url, path, job, cur.size, dl_prog)
                    validate_download(path, f.name, size, f.is_video)
                    break
                except (Cancelled, asyncio.CancelledError):
                    raise
                except Exception as e:
                    if str(e) == "TOO_BIG" or tried_alt or not (cur.source and not cur.fallback):
                        raise
                    log.warning("download failed (%s) — trying an alternate link", e)
                    tried_alt = True
                    await safe_edit(msg, f"🔁 <b>Link problem — switching to another server…</b>\n{title}", cancel_kb)
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
                    if not pthumb:
                        tp = os.path.join(workdir, f"thumb_{pi}.jpg")
                        pthumb = tp if await make_thumb(part, tp, duration) else None
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


async def on_download(client: Client, cq):
    _, rid, idx = cq.data.split(":")
    uid = cq.from_user.id
    f = _get_file(rid, int(idx), uid)
    if not f:
        await cq.answer("⌛ Expired — send the link again.", show_alert=True)
        return
    if not await precheck(client, cq):
        return
    if not f.download_url:
        await cq.answer("No download link available for this file.", show_alert=True)
        return
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
    if DAILY_LIMIT and not is_admin(uid):
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
            if DAILY_LIMIT and not is_admin(uid) and store.downloads_today(uid) >= DAILY_LIMIT:
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
        "📊 <b>Bot stats</b>\n\n"
        f"👥 Users: <b>{len(d['users'])}</b>\n⬇️ Downloads: <b>{d['total_downloads']}</b>\n"
        f"💾 Data sent: <b>{human_size(d['total_bytes'])}</b>\n🚫 Banned: <b>{len(d['banned'])}</b>\n"
        f"🔄 Active jobs: <b>{len(JOBS)}</b>\n⏱ Uptime: <b>{human_time(time.time() - START_TIME)}</b>",
        parse_mode=ParseMode.HTML)


async def on_ban(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    parts = message.text.split()
    if len(parts) < 2 or not parts[1].lstrip("-").isdigit():
        await message.reply_text("Usage: /ban <user_id> or /unban <user_id>")
        return
    uid = int(parts[1])
    on = parts[0].lstrip("/").split("@")[0].lower() == "ban"
    if on and is_admin(uid):
        await message.reply_text("Cannot ban an admin.")
        return
    store.ban(uid, on)
    await message.reply_text(f"{'🚫 Banned' if on else '✅ Unbanned'} <code>{uid}</code>", parse_mode=ParseMode.HTML)


async def on_broadcast(client: Client, message):
    if not is_admin(message.from_user.id):
        return
    src = message.reply_to_message
    if not src:
        await message.reply_text("Reply to a message with /broadcast to send it to all users.")
        return
    ids = store.user_ids()
    status = await message.reply_text(f"📣 Broadcasting to {len(ids)} users…")
    ok = fail = 0
    for i, uid in enumerate(ids, 1):
        try:
            await src.copy(uid)
            ok += 1
        except FloodWait as e:
            await asyncio.sleep(e.value + 1)
            try:
                await src.copy(uid)
                ok += 1
            except Exception:
                fail += 1
        except Exception:
            fail += 1
        if i % 25 == 0:
            await safe_edit(status, f"📣 Broadcasting… {i}/{len(ids)}")
        await asyncio.sleep(0.05)
    await safe_edit(status, f"✅ Broadcast finished\nSent: <b>{ok}</b>\nFailed: <b>{fail}</b>")


def status_text(started: bool, username: str) -> str:
    """fbot-style start / stop notice for the log channel."""
    ist = time.strftime("%I:%M %p IST", time.gmtime(time.time() + 5.5 * 3600))
    head = "🚀 <b>Bot successfully started!</b>" if started else "🛑 <b>Bot stopped!</b>"
    return (f"{head}\n\n"
            f"⭐ Bot: @{username}\n"
            f"👥 Users: {len(store.data['users'])}\n"
            f"⏳ Time: {ist}\n\n"
            f"👑 Developed by {DEVELOPER_URL.replace('https://t.me/', '@')}")


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
    app.add_handler(MessageHandler(on_help, filters.command("help") & private))
    app.add_handler(MessageHandler(on_about, filters.command("about") & private))
    app.add_handler(CallbackQueryHandler(on_about_close, filters.regex(r"^about_close$")))
    app.add_handler(MessageHandler(on_cancel, filters.command("cancel") & private))
    app.add_handler(MessageHandler(on_stats, filters.command("stats") & private))
    app.add_handler(MessageHandler(on_ban, filters.command(["ban", "unban"]) & private))
    app.add_handler(MessageHandler(on_broadcast, filters.command("broadcast") & private))
    app.add_handler(MessageHandler(on_link, filters.text & private & ~filters.command(["start", "help", "about", "cancel", "stats", "ban", "unban", "broadcast"])))
    app.add_handler(CallbackQueryHandler(on_verify, filters.regex(r"^verify$")))
    app.add_handler(CallbackQueryHandler(on_file_pick, filters.regex(r"^f:")))
    app.add_handler(CallbackQueryHandler(on_download, filters.regex(r"^dl:")))
    app.add_handler(CallbackQueryHandler(on_stream, filters.regex(r"^st:")))
    app.add_handler(CallbackQueryHandler(on_menu_back, filters.regex(r"^m:")))
    app.add_handler(CallbackQueryHandler(on_cancel_menu, filters.regex(r"^cx:")))
    app.add_handler(CallbackQueryHandler(on_download_all, filters.regex(r"^all:")))
    app.add_handler(CallbackQueryHandler(on_page, filters.regex(r"^p:")))
    app.add_handler(CallbackQueryHandler(on_cancel_btn, filters.regex(r"^x:")))

    await app.start()
    try:  # "/" command menu
        await app.set_bot_commands([BotCommand("start", "🚀 Start the bot"), BotCommand("help", "❓ How to use the bot"),
                                    BotCommand("about", "ℹ️ About this bot"), BotCommand("cancel", "🚫 Cancel current active download")])
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
