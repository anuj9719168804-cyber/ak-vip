"""Web server for Render: health check + /stream/<code> proxy + self-ping keep-alive.

Ported from fbot's keep_alive.py, rewritten on aiohttp so it shares the bot's event loop.
- GET/HEAD /, /health      -> 200 (port detection + keep-alive target)
- GET/HEAD /stream/<code>  -> reverse proxy of a registered CDN url, Range pass-through (seek works in browser / VLC)
- keep_alive_loop()        -> pings our own public url every 5 min so the free instance does not spin down
"""
import asyncio
import hashlib
import logging
import os
import time
import urllib.parse

import aiohttp
from aiohttp import web

log = logging.getLogger("terabox-bot")

STREAM_TTL = int(os.getenv("STREAM_PROXY_TTL", str(6 * 3600)))
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "300"))
MAX_ENTRIES = 2000

# code -> {"url", "name", "size", "ts"}
_registry: dict = {}

_MIME = {
    "mp4": "video/mp4", "mkv": "video/x-matroska", "webm": "video/webm", "mov": "video/quicktime",
    "avi": "video/x-msvideo", "m4v": "video/x-m4v", "ts": "video/mp2t", "flv": "video/x-flv",
    "mp3": "audio/mpeg", "m4a": "audio/mp4", "aac": "audio/aac", "ogg": "audio/ogg",
    "flac": "audio/flac", "wav": "audio/wav",
}

_proxy_headers: dict = {}
_session_getter = None


def public_base_url() -> str:
    """Public https url of this service (auto-detected on Render/Railway/Koyeb/Fly), or '' if unknown."""
    for env in ("PUBLIC_URL", "PING_URL", "RENDER_EXTERNAL_URL", "APP_URL"):
        v = os.getenv(env, "").strip().rstrip("/")
        if v:
            return v
    for env in ("RENDER_EXTERNAL_HOSTNAME", "RAILWAY_STATIC_URL", "KOYEB_PUBLIC_DOMAIN"):
        v = os.getenv(env, "").strip().strip("/")
        if v:
            return f"https://{v}"
    fly = os.getenv("FLY_APP_NAME", "").strip()
    return f"https://{fly}.fly.dev" if fly else ""


def register_stream(url: str, name: str, size: int = 0) -> str:
    """Register a CDN url and return the public /stream/<code> url ('' if no public url is known)."""
    base = public_base_url()
    if not base:
        return ""
    code = hashlib.sha1(url.encode()).hexdigest()[:12]
    now = time.time()
    _registry[code] = {"url": url, "name": name or "video.mp4", "size": size or 0, "ts": now}
    if len(_registry) > MAX_ENTRIES or len(_registry) % 50 == 0:
        for k in [k for k, v in _registry.items() if now - v["ts"] > STREAM_TTL]:
            _registry.pop(k, None)
        while len(_registry) > MAX_ENTRIES:
            _registry.pop(next(iter(_registry)), None)
    return f"{base}/stream/{code}"


async def _root(_req):
    return web.Response(text="Terabox bot is running")


async def _health(_req):
    return web.json_response({"status": "ok"})


async def _stream(req: web.Request):
    entry = _registry.get(req.match_info["code"])
    if not entry or time.time() - entry["ts"] > STREAM_TTL:
        return web.Response(status=404, text="Stream not found or expired.")
    name = entry["name"]
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    headers = dict(_proxy_headers)
    if req.headers.get("Range"):
        headers["Range"] = req.headers["Range"]
    session = _session_getter()
    try:
        up = await session.get(entry["url"], headers=headers, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=None, connect=15, sock_read=90))
    except Exception as e:
        log.warning("stream proxy fetch failed: %s", e)
        return web.Response(status=502, text=f"Upstream fetch failed: {e}")
    try:
        if up.status >= 400:
            return web.Response(status=up.status, text="Upstream error")
        ctype = up.headers.get("Content-Type", "")
        if not ctype.startswith(("video/", "audio/")):
            ctype = _MIME.get(ext, "video/mp4")
        safe = urllib.parse.quote(name)
        resp = web.StreamResponse(status=up.status)
        resp.content_type = ctype
        resp.headers["Content-Disposition"] = f"inline; filename*=UTF-8''{safe}"
        resp.headers["Accept-Ranges"] = "bytes"
        resp.headers["Cache-Control"] = "no-cache"
        for h in ("Content-Length", "Content-Range"):
            if up.headers.get(h):
                resp.headers[h] = up.headers[h]
        if "Content-Length" not in resp.headers and entry["size"] and not req.headers.get("Range"):
            resp.headers["Content-Length"] = str(entry["size"])
        await resp.prepare(req)
        if req.method == "HEAD":
            return resp
        try:
            async for chunk in up.content.iter_chunked(256 * 1024):
                await resp.write(chunk)
        except (ConnectionResetError, asyncio.CancelledError):
            raise
        except Exception as e:  # client gone / upstream stalled
            log.debug("stream write ended: %s", e)
        return resp
    finally:
        up.release()


async def start_web_server(port: int, session_getter, proxy_headers: dict) -> web.AppRunner:
    global _session_getter, _proxy_headers
    _session_getter, _proxy_headers = session_getter, dict(proxy_headers)
    app = web.Application()
    app.router.add_get("/", _root)
    app.router.add_get("/health", _health)
    app.router.add_get("/stream/{code}", _stream)
    runner = web.AppRunner(app, access_log=None)
    await runner.setup()
    await web.TCPSite(runner, "0.0.0.0", port).start()
    log.info("Web server listening on port %s (public url: %s)", port, public_base_url() or "unknown")
    return runner


async def keep_alive_loop(session_getter):
    """Self-ping so Render's free instance does not spin down after 15 min of inactivity."""
    base = public_base_url()
    if not base:
        log.info("Keep-alive: no public url (local/VPS run) - self-ping disabled.")
        return
    target = f"{base}/health"
    log.info("Keep-alive ping target: %s (every %ss)", target, PING_INTERVAL)
    await asyncio.sleep(30)  # let the server come up first
    fails = 0
    while True:
        try:
            async with session_getter().get(target, timeout=aiohttp.ClientTimeout(total=20)) as r:
                if fails:
                    log.info("Keep-alive recovered after %d failure(s): %s", fails, r.status)
                fails = 0
        except asyncio.CancelledError:
            raise
        except Exception as e:
            fails += 1
            if fails >= 3:
                log.warning("Keep-alive ping failed (%dx): %s", fails, e)
        await asyncio.sleep(PING_INTERVAL)
