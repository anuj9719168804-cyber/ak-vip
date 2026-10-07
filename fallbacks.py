"""Fallback resolvers, used ONLY when the primary playterabox API fails.

Order: flowvideoplayer.com -> azhawasadda.in -> terabox.beer.  Both resolve a single file (no folder listing).
Ported to async from the reference project's sync `requests` implementation.
"""
import asyncio
import logging
import random
import re
import time
from typing import Optional
from urllib.parse import quote, unquote, urlparse

import aiohttp

from terabox_api import TeraFile, TeraResult, TeraboxError

log = logging.getLogger("fallbacks")

# ------------------------------------------------------------------ helpers --
_SIZE_RE = re.compile(r"([\d.,]+)\s*(B|KB|MB|GB|TB)?", re.I)
_MULT = {"B": 1, "KB": 1024, "MB": 1024 ** 2, "GB": 1024 ** 3, "TB": 1024 ** 4}


def parse_size(v) -> int:
    """Accepts 12345, "12345", "12.3 MB", "1,5 GB" -> bytes (0 if unknown)."""
    if isinstance(v, (int, float)):
        return int(v)
    m = _SIZE_RE.fullmatch(str(v or "").strip())
    if not m:
        return 0
    try:
        return int(float(m.group(1).replace(",", ".")) * _MULT[(m.group(2) or "B").upper()])
    except Exception:
        return 0


def build_result(link: str, name: Optional[str], size=None, thumb: Optional[str] = None) -> TeraResult:
    name = (name or "terabox_video").strip() or "terabox_video"
    is_hls = ".m3u8" in link.lower()
    if is_hls and "." not in name.rsplit("/", 1)[-1]:
        name += ".mp4"
    elif is_hls and name.lower().endswith(".m3u8"):
        name = name[:-5] + ".mp4"
    f = TeraFile(name=name, size=parse_size(size), download_url=link, stream_url=link if is_hls else None,
                 m3u8_url=link if is_hls else None, thumb=thumb, fallback=True)
    return TeraResult(title=name, files=[f])


def extract_stream(raw: str) -> Optional[dict]:
    """Last-resort scan of a response body for a playable URL (.m3u8 first, then .mp4)."""
    text = (raw or "").replace("\\/", "/")
    for pat in (r'(https?://[^\s"\'<>]+\.m3u8[^\s"\'<>]*)', r'(https?://[^\s"\'<>]+\.mp4[^\s"\'<>]*)'):
        m = re.search(pat, text)
        if m:
            return {"link": m.group(1)}
    return None


def find_csrf(html_text: str) -> Optional[str]:
    for pat in (r'<meta[^>]+name=["\']csrf-token["\'][^>]+content=["\']([^"\']+)',
                r'<meta[^>]+content=["\']([^"\']+)["\'][^>]+name=["\']csrf-token'):
        m = re.search(pat, html_text, re.I)
        if m:
            return m.group(1)
    return None


# ------------------------------------------------------- flowvideoplayer.com --
FVP_SITE = "https://flowvideoplayer.com"
FVP_API = FVP_SITE + "/search/video"
FVP_UA = ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) "
          "Chrome/152.0.0.0 Mobile Safari/537.36")
FVP_TTL = 300
FVP_ATTEMPTS = 4


def _fvp_headers(csrf: str) -> dict:
    return {
        "Content-Type": "application/json", "Accept": "application/json", "X-Requested-With": "XMLHttpRequest",
        "X-CSRF-TOKEN": csrf, "User-Agent": FVP_UA, "Referer": FVP_SITE + "/", "Origin": FVP_SITE,
        "Sec-Fetch-Site": "same-origin", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Dest": "empty",
    }


class _FvpContext:
    """Cookie-holding session + CSRF token, shared across calls and refreshed on expiry / 419 / 401 / 403 / 'blocked'."""

    def __init__(self):
        self.session: Optional[aiohttp.ClientSession] = None
        self.csrf: Optional[str] = None
        self.created = 0.0
        self.lock = asyncio.Lock()

    async def invalidate(self):
        s, self.session, self.csrf, self.created = self.session, None, None, 0.0
        if s:
            await s.close()

    async def _fresh(self) -> bool:
        await self.invalidate()
        s = aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True), headers={
            "User-Agent": FVP_UA, "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Accept-Language": "en-US,en;q=0.5"})
        try:
            async with s.get(FVP_SITE, timeout=aiohttp.ClientTimeout(total=15)) as r:
                r.raise_for_status()
                page = await r.text()
            csrf = find_csrf(page)
            if not csrf:
                for c in s.cookie_jar:
                    if c.key == "XSRF-TOKEN":
                        csrf = unquote(c.value)
            if not csrf:
                raise TeraboxError("CSRF token not found")
            try:  # device fingerprint call the site requires before /search/video answers
                await s.post(FVP_SITE + "/device/init", headers=_fvp_headers(csrf), timeout=aiohttp.ClientTimeout(total=30), json={
                    "cpu": 8, "memory": 8, "touch": 0, "platform": "Linux x86_64", "lang": "en-US", "vendor": "Google Inc.",
                    "webgl_vendor": None, "webgl_renderer": None, "ua": FVP_UA, "backup_token": None, "os": "linux",
                    "browser": "chrome", "pwa_installed": False})
            except Exception as e:
                log.warning("flowvideoplayer device/init failed (continuing): %s", e)
        except Exception:
            await s.close()
            raise
        self.session, self.csrf, self.created = s, csrf, time.time()
        return True

    async def post_json(self, payload: dict):
        """Returns parsed JSON body. Raises TeraboxError if all attempts fail."""
        last = "unknown error"
        for attempt in range(FVP_ATTEMPTS):
            async with self.lock:
                if not self.session or time.time() - self.created >= FVP_TTL:
                    try:
                        await self._fresh()
                    except Exception as e:
                        last = f"could not get CSRF token: {e}"
                        await asyncio.sleep(0.7 * (attempt + 1))
                        continue
                s, csrf = self.session, self.csrf
            try:
                async with s.post(FVP_API, json=payload, headers=_fvp_headers(csrf),
                                  timeout=aiohttp.ClientTimeout(total=30)) as r:
                    status = r.status
                    body = await r.json(content_type=None) if status == 200 else None
            except Exception as e:
                last = f"network error: {e}"
                async with self.lock:
                    await self.invalidate()
                await asyncio.sleep(0.7 * (attempt + 1) + random.uniform(0, 0.3))
                continue
            blocked = isinstance(body, dict) and body.get("code") == 201 and "blocked" in str(body.get("message", "")).lower()
            if status in (419, 401, 403) or blocked:
                last = f"HTTP {status}" if not blocked else "device blocked"
                async with self.lock:
                    await self.invalidate()
                await asyncio.sleep(0.7 * (attempt + 1) + random.uniform(0, 0.3))
                continue
            if status != 200 or not isinstance(body, dict):
                raise TeraboxError(f"HTTP {status}")
            return body
        raise TeraboxError(f"CSRF/device token still failing ({last})")


_fvp = _FvpContext()


def parse_flowvideoplayer(data: dict) -> TeraResult:
    if not (data.get("code") == 200 and data.get("status") and data.get("response")):
        raise TeraboxError(str(data.get("message") or "no response data"))
    info = data["response"][0]
    link = info.get("download_url")
    if not link:
        raise TeraboxError("no download_url in response")
    return build_result(link, info.get("file_name"), info.get("file_size"), info.get("thumbnail") or info.get("thumb"))


async def resolve_flowvideoplayer(url: str, session=None) -> TeraResult:
    return parse_flowvideoplayer(await _fvp.post_json({"url": url}))


# ------------------------------------------------------------- terabox.beer --
BEER = "https://terabox.beer"
BEER_HEADERS = {
    "User-Agent": "Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/137.0.0.0 Mobile Safari/537.36",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,image/apng,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.9",
}
_BEER_FIELDS = ("stream_download_url", "download_link", "fallback_url", "proxy_url", "url", "video_url")


def parse_beer(api: dict, raw_text: str = "") -> tuple:
    """-> (link, name, size). Raises TeraboxError when nothing usable."""
    if not isinstance(api, dict):
        raise TeraboxError("API returned non-dict response")
    if api.get("error"):  # only a truthy error counts (missing/False = success)
        raise TeraboxError(f"API error: {api.get('error') or api.get('message')}")
    link = next((api[k] for k in _BEER_FIELDS if api.get(k)), None)
    if not link:
        link = next((v for v in api.values() if isinstance(v, str) and v.startswith(("http://", "https://"))), None)
    if not link:
        g = extract_stream(raw_text)
        link = g["link"] if g else None
    if not link:
        raise TeraboxError("no video URL found in API response")
    return link, api.get("file_name"), api.get("file_size")


async def resolve_beer(url: str, session=None) -> TeraResult:
    m = re.search(r"/s/([a-zA-Z0-9_-]+)", url)
    if not m:
        raise TeraboxError("could not extract video id from the link")
    vid = m.group(1)
    t = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession(cookie_jar=aiohttp.CookieJar(unsafe=True)) as s:
        # warm the session the same way the site does: home -> watch page -> API
        await s.get(BEER, headers=BEER_HEADERS | {"Referer": "https://www.google.com/"}, timeout=t, ssl=False)
        watch = f"{BEER}/watch/{vid}"
        await s.get(watch, headers=BEER_HEADERS | {"Referer": BEER + "/"}, timeout=t, ssl=False)
        async with s.get(f"{BEER}/api/terabox-new?link={quote(url, safe='')}", headers=BEER_HEADERS | {"Referer": watch},
                         timeout=t, ssl=False) as r:
            raw = await r.text()
            try:
                api = await r.json(content_type=None)
            except Exception:
                g = extract_stream(raw)
                if not g:
                    raise TeraboxError("failed to parse API response")
                return build_result(g["link"], None)
        link, name, size = parse_beer(api, raw)

        # follow redirects by hand, a page body on the way may expose a .m3u8
        cur = link
        for _ in range(5):
            try:
                async with s.get(cur, headers=BEER_HEADERS | {"Referer": BEER + "/"}, allow_redirects=False,
                                 timeout=t, ssl=False) as rr:
                    if rr.status in (301, 302, 303, 307, 308) and rr.headers.get("Location"):
                        loc = rr.headers["Location"]
                        if loc.startswith("/"):
                            p = urlparse(cur)
                            loc = f"{p.scheme}://{p.netloc}{loc}"
                        cur = loc
                        continue
                    ctype = (rr.headers.get("Content-Type") or "").lower()
                    if "text" in ctype or "json" in ctype or "mpegurl" in ctype:
                        g = extract_stream(await rr.text())
                        if g and ".m3u8" in g["link"]:
                            cur = g["link"]
                    break
            except Exception:
                break
        return build_result(cur if ".m3u8" in cur else link, name, size)


AZHA_API = "https://azhawasadda.in/api/extract"
AZHA_UA = ("Mozilla/5.0 (Linux; Android 10; K) AppleWebKit/537.36 (KHTML, like Gecko) "
           "Chrome/139.0.0.0 Mobile Safari/537.36")


async def resolve_azhawasadda(url: str, session=None) -> TeraResult:
    """azhawasadda.in -- free, no key. Returns a direct mp4 link (Range supported). Single file only."""
    t = aiohttp.ClientTimeout(total=30)
    async with aiohttp.ClientSession() as s:
        async with s.get(AZHA_API, params={"url": url}, headers={"User-Agent": AZHA_UA, "Accept": "*/*"},
                         timeout=t) as r:
            if r.status >= 400:
                raise TeraboxError(f"HTTP {r.status}")
            try:
                data = await r.json(content_type=None)
            except Exception:
                raise TeraboxError("couldn't parse API response")
    if not isinstance(data, dict):
        raise TeraboxError("unexpected API response")
    if data.get("errno"):
        raise TeraboxError(str(data.get("errmsg") or f"errno {data.get('errno')}"))
    f = (data.get("data") or {}).get("file") or {}
    link = f.get("direct_link") or f.get("download_url")
    if not link:
        raise TeraboxError("no download link in API response")
    return build_result(link, f.get("file_name"), f.get("size_readable") or data.get("total_size"), f.get("thumbnail"))


# Tried in order, only after the primary API failed.
FALLBACKS = (
    ("flowvideoplayer", resolve_flowvideoplayer),
    ("azhawasadda", resolve_azhawasadda),
    ("terabox.beer", resolve_beer),
)


async def resolve_with_fallbacks(url: str, session=None) -> TeraResult:
    errors = []
    for name, fn in FALLBACKS:
        try:
            res = await asyncio.wait_for(fn(url, session), timeout=90)
            log.info("fallback %s resolved", name)
            return res
        except Exception as e:
            log.warning("fallback %s failed: %s", name, e)
            errors.append(f"{name}: {str(e)[:120] or e.__class__.__name__}")
    raise TeraboxError(" | ".join(errors))
