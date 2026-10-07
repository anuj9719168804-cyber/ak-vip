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
import re
import time
import urllib.parse
from collections import deque

import aiohttp
from aiohttp import web

log = logging.getLogger("terabox-bot")

STREAM_TTL = int(os.getenv("STREAM_PROXY_TTL", str(6 * 3600)))
PING_INTERVAL = int(os.getenv("PING_INTERVAL", "300"))
MAX_ENTRIES = 2000

# Parallel ranged relay. A single CDN connection is throttled (Terabox: ~2-4 MB/s each, but several simultaneous Range requests
# are allowed), so the proxy fetches PROXY_CONN chunks at once and hands them to the player in order (same idea as fbot's
# multi-connection downloader). STREAM_PROXY_CONN=1 turns it off (plain single-connection relay).
PROXY_CONN = max(1, int(os.getenv("STREAM_PROXY_CONN", "6")))
PROXY_CHUNK = max(256 * 1024, int(float(os.getenv("STREAM_PROXY_CHUNK_MB", "2")) * 1024 * 1024))
PROXY_FIRST = 512 * 1024  # small first chunk -> playback starts quickly
PROXY_RETRIES = 3
PROXY_MAX_UPSTREAM = max(2, int(os.getenv("STREAM_PROXY_MAX_UPSTREAM", "32")))  # all viewers together
_up_sem = None

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
        log.error("Cannot register stream: no public URL detected. Set PUBLIC_URL, RENDER_EXTERNAL_URL, or similar env var")
        return ""
    
    if not url or not url.startswith("http"):
        log.error("Cannot register stream: invalid URL: %s", url[:100] if url else "None")
        return ""
    
    code = hashlib.sha1(url.encode()).hexdigest()[:12]
    now = time.time()
    _registry[code] = {"url": url, "name": name or "video.mp4", "size": size or 0, "ts": now}
    
    # Cleanup old entries
    if len(_registry) > MAX_ENTRIES or len(_registry) % 50 == 0:
        before = len(_registry)
        for k in [k for k, v in _registry.items() if now - v["ts"] > STREAM_TTL]:
            _registry.pop(k, None)
        while len(_registry) > MAX_ENTRIES:
            _registry.pop(next(iter(_registry)), None)
        if before != len(_registry):
            log.info("Registry cleanup: %d -> %d entries", before, len(_registry))
    
    stream_url = f"{base}/stream/{code}"
    log.info("Stream registered: %s (code: %s, name: %s, size: %s, registry: %d/%d)", 
             stream_url, code, name[:50], size, len(_registry), MAX_ENTRIES)
    return stream_url


async def _root(_req):
    return web.Response(text="Terabox bot is running")


async def _health(_req):
    return web.json_response({"status": "ok"})


async def _stream_simple(req: web.Request):
    code = req.match_info["code"]
    entry = _registry.get(code)
    if not entry:
        log.warning("Stream code not in registry: %s (total entries: %d)", code, len(_registry))
        return web.Response(status=404, text="Stream not found (code invalid or expired)")
    
    if time.time() - entry["ts"] > STREAM_TTL:
        age = time.time() - entry["ts"]
        log.warning("Stream code expired: %s (age: %d sec, TTL: %d)", code, age, STREAM_TTL)
        return web.Response(status=404, text="Stream not found (expired)")
    
    name = entry["name"]
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    headers = dict(_proxy_headers)
    if req.headers.get("Range"):
        headers["Range"] = req.headers["Range"]
    
    session = _session_getter()
    try:
        log.debug("Fetching upstream: %s (code: %s)", entry["url"][:100], code)
        up = await session.get(entry["url"], headers=headers, allow_redirects=True,
                               timeout=aiohttp.ClientTimeout(total=None, connect=20, sock_read=120))
    except asyncio.TimeoutError as e:
        log.error("Stream timeout: %s", e)
        return web.Response(status=504, text=f"Upstream timeout: {str(e)[:100]}")
    except Exception as e:
        log.error("Stream proxy fetch failed: %s (type: %s)", e, type(e).__name__)
        return web.Response(status=502, text=f"Upstream fetch failed: {str(e)[:100]}")
    
    try:
        if up.status >= 400:
            log.error("Upstream returned error: HTTP %d for %s", up.status, entry["url"][:100])
            body = await up.text()
            log.error("Upstream error body: %s", body[:200])
            return web.Response(status=up.status, text=f"Upstream error: HTTP {up.status}")
        
        ctype = up.headers.get("Content-Type", "")
        if not ctype.startswith(("video/", "audio/")):
            ctype = _MIME.get(ext, "video/mp4")
        
        safe = urllib.parse.quote(name)
        resp = web.StreamResponse(status=up.status)
        resp.content_type = ctype
        resp.headers["Content-Disposition"] = f"inline; filename*=UTF-8''{safe}"
        resp.headers["Accept-Ranges"] = "bytes"
        resp.headers["Cache-Control"] = "no-cache"
        resp.headers["Access-Control-Allow-Origin"] = "*"
        
        for h in ("Content-Length", "Content-Range"):
            if up.headers.get(h):
                resp.headers[h] = up.headers[h]
        if "Content-Length" not in resp.headers and entry["size"] and not req.headers.get("Range"):
            resp.headers["Content-Length"] = str(entry["size"])
        
        await resp.prepare(req)
        if req.method == "HEAD":
            return resp
        
        bytes_written = 0
        try:
            async for chunk in up.content.iter_chunked(256 * 1024):
                await resp.write(chunk)
                bytes_written += len(chunk)
        except asyncio.CancelledError:
            raise
        except ConnectionResetError:  # player closed / seeked: normal, not an error
            log.debug("Stream client disconnected after %d bytes", bytes_written)
            return resp
        except Exception as e:  # client gone / upstream stalled
            log.debug("Stream write ended: %s (bytes written: %d)", e, bytes_written)
        return resp
    finally:
        up.release()


def _parse_range(h):
    """Range header -> (start, end|None); (0, None) when absent; None for forms we do not split (suffix / multi / junk)."""
    if not h:
        return 0, None
    m = re.fullmatch(r"bytes=(\d+)-(\d*)", h.strip())
    if not m:
        return None
    a, b = int(m.group(1)), (int(m.group(2)) if m.group(2) else None)
    return (a, b) if b is None or b >= a else None


def _total_from(headers) -> int:
    tail = (headers.get("Content-Range") or "").rsplit("/", 1)[-1].strip()
    return int(tail) if tail.isdigit() else 0


def _sem() -> asyncio.Semaphore:
    global _up_sem
    if _up_sem is None:
        _up_sem = asyncio.Semaphore(PROXY_MAX_UPSTREAM)
    return _up_sem


async def _fetch_range(session, url: str, headers: dict, a: int, b: int):
    h = dict(headers)
    h["Range"] = f"bytes={a}-{b}"
    async with session.get(url, headers=h, allow_redirects=True,
                           timeout=aiohttp.ClientTimeout(total=90, connect=15, sock_read=30)) as r:
        body = await r.read()
        return r.status, r.headers, body


async def _fetch_chunk(session, url: str, headers: dict, a: int, b: int) -> bytes:
    """One exact [a, b] slice, retried; raises if the CDN keeps failing."""
    want, err = b - a + 1, "unknown"
    for attempt in range(PROXY_RETRIES):
        try:
            async with _sem():
                st, _h, body = await _fetch_range(session, url, headers, a, b)
            if st == 206 and len(body) == want:
                return body
            err = f"HTTP {st}, got {len(body)} of {want} bytes"
        except asyncio.CancelledError:
            raise
        except Exception as e:
            err = str(e) or e.__class__.__name__
        await asyncio.sleep(0.3 * (attempt + 1))
    raise RuntimeError(f"chunk {a}-{b} failed: {err}")


async def _stream_parallel(req: web.Request, entry: dict, rng):
    """Serve the requested range through PROXY_CONN parallel upstream Range requests, in order.
    Returns None (nothing sent yet) when the upstream does not do ranges / the first chunk fails -> caller falls back to the plain relay."""
    start, end = rng
    has_range = bool(req.headers.get("Range"))
    session, url, base = _session_getter(), entry["url"], dict(_proxy_headers)
    first_end = start + PROXY_FIRST - 1 if end is None else min(start + PROXY_FIRST - 1, end)
    try:
        st, h, body = await _fetch_range(session, url, base, start, first_end)
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.info("parallel relay: first chunk failed (%s) -> plain relay", e)
        return None
    total = _total_from(h) if st == 206 else 0
    if st != 206 or not total or not body:
        log.info("parallel relay: upstream gave HTTP %s / no total -> plain relay", st)
        return None
    if start >= total:
        return web.Response(status=416, headers={"Content-Range": f"bytes */{total}"})
    last = total - 1 if end is None else min(end, total - 1)
    body = body[: last - start + 1]

    name = entry["name"]
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
    ctype = h.get("Content-Type", "")
    if not ctype.startswith(("video/", "audio/")):
        ctype = _MIME.get(ext, "video/mp4")
    resp = web.StreamResponse(status=206 if has_range else 200)
    resp.content_type = ctype
    resp.headers["Content-Disposition"] = f"inline; filename*=UTF-8''{urllib.parse.quote(name)}"
    resp.headers["Accept-Ranges"] = "bytes"
    resp.headers["Cache-Control"] = "no-cache"
    resp.headers["Access-Control-Allow-Origin"] = "*"
    resp.headers["Content-Length"] = str(last - start + 1)
    if has_range:
        resp.headers["Content-Range"] = f"bytes {start}-{last}/{total}"
    await resp.prepare(req)

    pending: deque = deque()
    nxt = start + len(body)

    def schedule():
        nonlocal nxt
        while nxt <= last and len(pending) < PROXY_CONN:
            b = min(nxt + PROXY_CHUNK - 1, last)
            pending.append(asyncio.create_task(_fetch_chunk(session, url, base, nxt, b)))
            nxt = b + 1

    try:
        schedule()  # next chunks download while the first one is being written
        await resp.write(body)
        while pending:
            data = await pending.popleft()
            schedule()  # keep the window full: the next chunk is already downloading while this one is written
            await resp.write(data)
    except ConnectionResetError:  # player closed / seeked: normal
        log.debug("parallel relay: client disconnected")
    except asyncio.CancelledError:
        raise
    except Exception as e:  # CDN kept failing mid-stream: cut the connection so the player retries
        log.warning("parallel relay aborted: %s", e)
        if req.transport:
            req.transport.close()
    finally:
        for t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
    return resp


async def _stream(req: web.Request):
    code = req.match_info["code"]
    entry = _registry.get(code)
    if entry and PROXY_CONN > 1 and req.method == "GET" and time.time() - entry["ts"] <= STREAM_TTL:
        rng = _parse_range(req.headers.get("Range"))
        if rng is not None:
            resp = await _stream_parallel(req, entry, rng)
            if resp is not None:
                return resp
    return await _stream_simple(req)  # HEAD, suffix ranges, upstream without Range support, unknown code (404) ...


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
