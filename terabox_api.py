"""Client for the playterabox.com fetch API + helpers (link detection, response parsing)."""
import asyncio
import hashlib
import re
import time
from dataclasses import dataclass, field
from typing import List, Optional
from urllib.parse import parse_qs, quote, urlparse

import aiohttp

# Values come from the API provider. Override through env if they ever rotate (see bot.py / .env).
SECRET_SALT = "T9do@SM1?xGn5"
API_BASE = "https://playterabox.com/api/fetch-video"
TOKEN_PATH = "/api/stream.php"

# Explicit domains (any path is accepted on these). Subdomains (www., dm., app., ...) always match.
TERABOX_HOSTS = (
    "terabox.com", "teraboxapp.com", "terabox.app", "terabox.club", "terabox.fun", "terabox.link", "terabox.click",
    "terabox.tech", "terabox1.com", "iterabox.com", "freeterabox.com", "teraboxfree.com", "teraboxlite.com",
    "1024terabox.com", "1024-terabox.com", "1024tera.com", "1024tera.co", "tera1024box.com",
    "terafileshare.com", "teraboxshare.com", "teraboxsharefile.com", "terashareus.com", "terashare.me",
    "terasharelink.com", "terasharefile.com", "teraboxlink.com", "teraboxurl.com", "teraboxfile.com",
    "teraboxshort.com", "teraboxshortlink.com", "urlshortterabox.com", "teraboxdownloader.online", "terafile.co",
    "4funbox.com", "4funbox.co", "4funbox.in", "fancybox.in", "mirrobox.com", "momerybox.com", "nephobox.com",
    "tibibox.com", "gibibox.com", "goaibox.com", "joybox.cc", "pebibox.com", "bestclouddrive.com",
    "dubox.com", "dubox.cc", "gcloud.live", "xtrabox.com", "boxlink.me",
)
# Unknown/new mirrors: accept when the domain name contains one of these AND the path looks like a share link.
_BRAND_HINTS = ("tera", "4funbox", "mirrobox", "nephobox", "momerybox", "tibibox", "dubox", "gibibox", "goaibox", "joybox", "pebibox", "fancybox", "bestclouddrive")
_SHARE_PATHS = ("/s/", "/wap/share/", "/sharing/", "/web/share/", "/share/link", "/share/init", "/yun/")
_URL_RE = re.compile(r"(?:https?://)?(?:[a-z0-9-]+\.)+[a-z]{2,}(?::\d+)?/[^\s<>\"']*", re.I)


def _sld(host: str) -> str:
    parts = host.split(".")
    return parts[-2] if len(parts) >= 2 else host


def _is_tera_host(host: str) -> bool:
    return any(host == h or host.endswith("." + h) for h in TERABOX_HOSTS)


def _normalize(url: str, host: str, parsed) -> str:
    """wap/share/filelist?surl=XXXX and sharing/link?surl=XXXX -> https://host/s/1XXXX (+ keep ?dir=)."""
    qs = parse_qs(parsed.query)
    surl = (qs.get("surl") or [""])[0]
    if surl and not parsed.path.startswith("/s/"):
        out = f"https://{host}/s/{'' if surl.startswith('1') else '1'}{surl}"
        if qs.get("dir"):
            out += "?dir=" + quote(qs["dir"][0])
        return out
    return url


def extract_terabox_url(text: str) -> Optional[str]:
    """Return the first Terabox-looking URL in `text` (or None). Works with any known/unknown Terabox mirror."""
    for m in _URL_RE.finditer(text or ""):
        url = m.group(0).rstrip(").,;:!]>")
        if not url.lower().startswith("http"):
            url = "https://" + url
        p = urlparse(url)
        host = (p.hostname or "").lower()
        if not host:
            continue
        looks_share = p.path.lower().startswith(_SHARE_PATHS) or "surl=" in p.query.lower()
        if _is_tera_host(host) or (looks_share and any(h in _sld(host) for h in _BRAND_HINTS)):
            return _normalize(url, host, p)
    return None


def generate_token(salt: str = SECRET_SALT, path: str = TOKEN_PATH):
    timestamp = int(time.time())
    raw = f"{salt}{timestamp}{path}"
    return hashlib.md5(raw.encode()).hexdigest(), timestamp


def _to_int(v) -> int:
    if isinstance(v, (int, float)):
        return int(v)
    try:
        return int(float(str(v).strip()))
    except Exception:
        return 0


@dataclass
class TeraFile:
    name: str
    size: int
    download_url: Optional[str]
    stream_url: Optional[str]
    m3u8_url: Optional[str]
    thumb: Optional[str]
    is_dir: bool = False
    path: str = ""
    source: str = ""        # share URL, set only for single-file shares (enables alternate-link retry)
    fallback: bool = False  # True when this link already came from a fallback resolver
    duration: int = 0       # seconds, when the API provides it
    ctime: int = 0          # unix upload time, when the API provides it

    @property
    def is_video(self) -> bool:
        return self.name.lower().rsplit(".", 1)[-1] in {"mp4", "mkv", "mov", "avi", "webm", "m4v", "ts", "flv", "3gp"}


@dataclass
class TeraResult:
    title: str = ""
    files: List[TeraFile] = field(default_factory=list)


def _to_secs(v) -> int:
    """'90', 90, 90.5 or '1:30:05' -> seconds (0 if unknown)."""
    try:
        if isinstance(v, str) and ":" in v:
            secs = 0
            for part in v.split(":"):
                secs = secs * 60 + int(float(part))
            return secs
        n = float(v or 0)
        return int(n / 1000) if n > 86400 * 2 else int(n)  # tolerate milliseconds
    except Exception:
        return 0


def parse_result(data: dict) -> TeraResult:
    res = TeraResult(title=str(data.get("title") or data.get("share_title") or ""))
    for it in data.get("list") or []:
        if not isinstance(it, dict):
            continue
        thumb = it.get("thumbnail") or it.get("thumb") or it.get("thumbnails") or it.get("image")
        if isinstance(thumb, dict):  # {"url3": "...", ...}
            pri = ["url3", "url2", "url1", "icon"]  # url3 is the largest variant
            vals = sorted(thumb.items(), key=lambda kv: pri.index(kv[0]) if kv[0] in pri else len(pri))
            thumb = next((v for _, v in vals if isinstance(v, str) and v.startswith("http")), None)
        res.files.append(TeraFile(
            name=str(it.get("name") or it.get("server_filename") or "terabox_file"),
            size=_to_int(it.get("size")),
            download_url=it.get("download_link") or it.get("normal_dlink") or it.get("dlink"),
            stream_url=it.get("stream_url"),
            m3u8_url=it.get("m3u8_url"),
            thumb=thumb if isinstance(thumb, str) else None,
            is_dir=str(it.get("is_dir") or it.get("isdir") or "0") in ("1", "true", "True"),
            path=str(it.get("path") or ""),
            duration=_to_secs(it.get("duration") or it.get("video_duration") or it.get("play_time")),
            ctime=_to_int(it.get("server_ctime") or it.get("create_time") or it.get("ctime")),
        ))
    return res


class TeraboxError(Exception):
    pass


PRIMARY_TIMEOUT = 15  # seconds per attempt (was 60 -> bot looked frozen for minutes)


async def fetch_primary(url: str, retries: int = 1, session: Optional[aiohttp.ClientSession] = None) -> TeraResult:
    headers = {
        "Content-Type": "application/json",
        "Origin": "https://playterabox.com",
        "Referer": "https://playterabox.com/",
        "Accept": "*/*",
    }
    own = session is None
    session = session or aiohttp.ClientSession()
    last: Exception = TeraboxError("unknown error")
    try:
        for attempt in range(retries + 1):
            token, ts = generate_token()  # fresh token each try (timestamp-bound)
            try:
                async with session.post(
                    f"{API_BASE}?token={token}&t={ts}", headers=headers, json={"url": url},
                    timeout=aiohttp.ClientTimeout(total=PRIMARY_TIMEOUT),
                ) as r:
                    if r.status >= 400:
                        raise TeraboxError(f"HTTP {r.status}")
                    data = await r.json(content_type=None)
                if not isinstance(data, dict) or data.get("status") != "success":
                    raise TeraboxError(str(data)[:300])
                res = parse_result(data)
                if not res.files:
                    raise TeraboxError("API returned no files")
                return res
            except (aiohttp.ClientError, asyncio.TimeoutError, TeraboxError, ValueError) as e:  # ValueError = non-JSON (Cloudflare/HTML) body
                last = e if str(e) else TeraboxError(e.__class__.__name__)
                if attempt < retries:
                    await asyncio.sleep(1.0)
        raise TeraboxError(str(last) or last.__class__.__name__)
    finally:
        if own:
            await session.close()


async def fetch_terabox(url: str, retries: int = 1, session: Optional[aiohttp.ClientSession] = None,
                        fallback: bool = True) -> TeraResult:
    """Primary API first; flowvideoplayer / terabox.beer only if it failed (single-file results)."""
    try:
        return await fetch_primary(url, retries=retries, session=session)
    except Exception as primary_err:
        if not fallback:
            raise
        from fallbacks import resolve_with_fallbacks  # lazy: avoids a circular import
        try:
            return await resolve_with_fallbacks(url.split("?")[0], session)
        except Exception as fb_err:
            raise TeraboxError(f"primary: {str(primary_err)[:150]} | fallbacks: {fb_err}")


async def list_folder(url: str, first: TeraResult, session=None, max_files: int = 100, max_dirs: int = 40):
    """Walk a folder share (breadth-first) and return (files, skipped_dirs).

    Subfolders are requested as `<share url>?dir=<path>` (same scheme the reference project uses).
    A failing subfolder is skipped, never fatal.
    """
    base = url.split("?")[0]
    files: List[TeraFile] = []
    queue: List[str] = []

    def take(res: TeraResult):
        for f in res.files:
            if f.is_dir:
                if f.path:
                    queue.append(f.path)
            elif f.download_url or f.stream_url:
                files.append(f)

    take(first)
    visited = 0
    while queue and len(files) < max_files and visited < max_dirs:
        d = queue.pop(0)
        visited += 1
        try:
            take(await fetch_terabox(f"{base}?dir={quote(d)}", retries=1, session=session, fallback=False))
        except Exception:
            continue
    return files[:max_files], len(queue)
