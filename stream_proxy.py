"""Web server for Render: health check + /stream/<code> proxy + self-ping keep-alive.

Ported from fbot's keep_alive.py, rewritten on aiohttp so it shares the bot's event loop.
- GET/HEAD /, /health      -> 200 (port detection + keep-alive target)
- GET/HEAD /stream/<code>  -> 302 redirect to a registered CDN url (or relay with Range pass-through)
- keep_alive_loop()        -> pings our own public url every 5 min so the free instance does not spin down
"""
import asyncio
import base64
import hashlib
import hmac
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
# STREAM_PROXY_MODE: "redirect" (default) -> /stream/<code> 302-redirects to the CDN for players like VLC / MX Player (full CDN speed,
#                    nothing flows through this server). A browser opening the link is relayed through this server instead, with inline
#                    headers, so it plays the file rather than downloading it (the CDN sends Content-Disposition: attachment).
#                    "relay"    -> bytes are relayed through this server with inline headers (slow on a free host; last resort).
#                    ?relay=1 on any /stream link forces the relay for that request.
PROXY_MODE = os.getenv("STREAM_PROXY_MODE", "relay").strip().lower()
PROXY_FIRST = 512 * 1024  # small first chunk -> playback starts quickly
PROXY_RETRIES = 3
PROXY_MAX_UPSTREAM = max(2, int(os.getenv("STREAM_PROXY_MAX_UPSTREAM", "32")))  # all viewers together
_up_sem = None

# ---- Disk chunk cache -------------------------------------------------------------------------------------------------
# Every chunk the relay downloads is also written to disk (aligned CACHE_CHUNK grid). When the same video is streamed again
# (replay, seek back, second viewer) the bytes come from disk instead of the CDN -> no re-buffering.
# STREAM_CACHE=0 turns it off. STREAM_CACHE_MAX_MB caps total disk use (oldest-used files are evicted first).
CACHE_ON = os.getenv("STREAM_CACHE", "1").strip().lower() not in ("0", "false", "no", "off")
CACHE_DIR = os.getenv("STREAM_CACHE_DIR", "/tmp/stream_cache")
CACHE_MAX = max(64, int(os.getenv("STREAM_CACHE_MAX_MB", "1024"))) * 1024 * 1024
CACHE_CHUNK = PROXY_CHUNK  # grid size; chunks are stored as <CACHE_DIR>/<key>/<index>
_cache_bytes = 0  # running estimate of disk use
_cache_inflight: dict = {}  # (key, idx) -> Task, so two viewers never download the same chunk twice
_meta: dict = {}  # key -> (total_size, content_type) learned from the CDN
# Background pre-download: as soon as a stream is registered the whole file (up to PREFETCH_MAX_MB, default 60% of the cache)
# is pulled onto disk, one video at a time, a few chunks in parallel (leaves bandwidth for the viewer). Replays then never wait.
PREFETCH_ON = os.getenv("STREAM_PREFETCH", "0").strip().lower() not in ("0", "false", "no", "off")
PREFETCH_PAR = max(1, int(os.getenv("STREAM_PREFETCH_CONN", "4")))
_prefetching: set = set()
# STREAM_PIPE=1 (default): bytes are piped to the player AS THEY ARRIVE from the CDN (one connection, Range pass-through - no waiting
# for whole chunks) and are copied to the disk cache on the way; a replay is served from disk. STREAM_PIPE=0 -> old parallel chunk relay.
PIPE_ON = os.getenv("STREAM_PIPE", "1").strip().lower() not in ("0", "false", "no", "off")
_pf_lock = None


def _cache_key(entry: dict) -> str:
    """Same file -> same key, even if the CDN url (and so the /stream code) changed. Falls back to the url when size is unknown."""
    if entry.get("size"):
        raw = f"{entry['name']}|{entry['size']}"
    else:
        raw = entry["url"]
    return hashlib.sha1(raw.encode()).hexdigest()[:16]


def _cache_path(key: str, idx: int) -> str:
    return os.path.join(CACHE_DIR, key, str(idx))


def _cache_get(key: str, idx: int, want: int):
    """Cached chunk bytes, or None. A file with the wrong length (partial / other size grid) is ignored."""
    if not CACHE_ON:
        return None
    path = _cache_path(key, idx)
    try:
        with open(path, "rb") as f:
            data = f.read()
        if len(data) != want:
            return None
        os.utime(path, None)  # LRU: mark as recently used
        return data
    except OSError:
        return None


def _has_cache(entry: dict) -> bool:
    """True when the start of this video is already on disk (so it is worth serving through the relay instead of redirecting)."""
    return CACHE_ON and os.path.exists(_cache_path(_cache_key(entry), 0))


def _cache_put(key: str, idx: int, data: bytes) -> None:
    global _cache_bytes
    if not CACHE_ON or not data:
        return
    path = _cache_path(key, idx)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        tmp = f"{path}.{os.getpid()}.tmp"
        with open(tmp, "wb") as f:
            f.write(data)
        os.replace(tmp, path)  # atomic: readers never see half a chunk
        _cache_bytes += len(data)
        if _cache_bytes > CACHE_MAX:
            _cache_evict()
    except OSError as e:
        log.debug("stream cache write failed: %s", e)


def _cache_evict() -> None:
    """Delete least-recently-used chunks until the cache is ~80% of the limit."""
    global _cache_bytes
    files = []
    total = 0
    for root, _d, names in os.walk(CACHE_DIR):
        for n in names:
            fp = os.path.join(root, n)
            try:
                st = os.stat(fp)
            except OSError:
                continue
            if n.endswith(".tmp"):
                if time.time() - st.st_mtime > 600:  # stale temp file from a crash
                    try:
                        os.remove(fp)
                    except OSError:
                        pass
                continue
            files.append((st.st_atime, st.st_size, fp))
            total += st.st_size
    target = int(CACHE_MAX * 0.8)
    if total > target:
        files.sort()
        for _at, sz, fp in files:
            if total <= target:
                break
            try:
                os.remove(fp)
                total -= sz
            except OSError:
                pass
        for d in os.listdir(CACHE_DIR):  # drop empty per-video folders
            try:
                os.rmdir(os.path.join(CACHE_DIR, d))
            except OSError:
                pass
        log.info("stream cache evicted down to %.0f MB", total / 1048576)
    _cache_bytes = total


def _cache_init() -> None:
    """Start clean on boot (a restarted free instance has a wiped /tmp anyway) and measure what is there."""
    global _cache_bytes
    if not CACHE_ON:
        return
    try:
        os.makedirs(CACHE_DIR, exist_ok=True)
        _cache_bytes = 0
        _cache_evict()
        log.info("Stream cache on: %s (max %d MB, chunk %d KB)", CACHE_DIR, CACHE_MAX // 1048576, CACHE_CHUNK // 1024)
    except OSError as e:
        log.warning("Stream cache disabled (cannot use %s): %s", CACHE_DIR, e)

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
    
    _start_prefetch(_registry[code])
    stream_url = f"{base}/stream/{code}"
    log.info("Stream registered: %s (code: %s, name: %s, size: %s, registry: %d/%d)", 
             stream_url, code, name[:50], size, len(_registry), MAX_ENTRIES)
    return stream_url


# ---- HLS (m3u8) proxy + segment cache ---------------------------------------------------------------------------------
# Some Terabox files only give an HLS playlist. /hls/<code>/master.m3u8 fetches it through this server, rewrites every URL
# inside to /hls/<code>/p/<token> and pipes segments back as they arrive, keeping each finished segment on disk -> replays and
# seeks back are served from disk. Tokens are HMAC-signed, so only URLs found in a playlist we served can ever be fetched
# (no open proxy / SSRF).
_HLS_SECRET = os.urandom(16)
_HLS_MIME = "application/vnd.apple.mpegurl"
_HLS_MAX_SEG = 64 * 1024 * 1024  # bigger single responses are piped but not cached
_URI_ATTR_RE = re.compile(r'URI="([^"]*)"')
_CORS = {"Access-Control-Allow-Origin": "*", "Access-Control-Allow-Methods": "GET, HEAD, OPTIONS",
         "Access-Control-Allow-Headers": "Range, Content-Type",
         "Access-Control-Expose-Headers": "Content-Length, Content-Range, Accept-Ranges"}


def register_hls(url: str, name: str) -> str:
    """Register an HLS playlist url -> public /hls/<code>/master.m3u8 ('' if no public url)."""
    base = public_base_url()
    if not base or not url or not url.startswith("http"):
        return ""
    code = "h" + hashlib.sha1(url.encode()).hexdigest()[:11]
    _registry[code] = {"url": url, "name": name or "video", "size": 0, "ts": time.time(), "kind": "hls"}
    log.info("HLS stream registered: %s/hls/%s/master.m3u8 (name: %s)", base, code, (name or "")[:50])
    return f"{base}/hls/{code}/master.m3u8"


def _sig(code: str, url: str) -> str:
    return hmac.new(_HLS_SECRET, f"{code}|{url}".encode(), hashlib.sha256).hexdigest()[:16]


def _hls_path(code: str, url: str) -> str:
    m = re.search(r"\.([A-Za-z0-9]{1,5})$", urllib.parse.urlparse(url).path)
    tok = base64.urlsafe_b64encode(url.encode()).decode().rstrip("=")
    return f"/hls/{code}/p/{tok}~{_sig(code, url)}{'.' + m.group(1).lower() if m else ''}"


def _hls_url_from(code: str, token: str):
    """Signed token -> upstream url, or None when malformed / not signed by us."""
    try:
        tok, sig = token.split(".", 1)[0].rsplit("~", 1)
        url = base64.urlsafe_b64decode(tok + "=" * (-len(tok) % 4)).decode()
    except Exception:
        return None
    return url if hmac.compare_digest(sig, _sig(code, url)) and url.startswith(("http://", "https://")) else None


def _rewrite_playlist(text: str, playlist_url: str, code: str) -> str:
    def prox(uri: str) -> str:
        uri = uri.strip()
        sch = urllib.parse.urlparse(uri).scheme.lower()
        if not uri or (sch and sch not in ("http", "https")):
            return uri
        return _hls_path(code, urllib.parse.urljoin(playlist_url, uri))

    out = []
    for raw in text.split("\n"):
        line = raw.rstrip("\r")
        st = line.strip()
        if not st:
            out.append(line)
        elif st.startswith("#"):
            out.append(_URI_ATTR_RE.sub(lambda m: f'URI="{prox(m.group(1))}"', line) if "URI=" in line else line)
        else:
            out.append(prox(st))
    return "\n".join(out)


def _seg_get(key: str):
    if not CACHE_ON:
        return None
    path = _cache_path(key, 0)
    try:
        with open(path, "rb") as f:
            data = f.read()
        os.utime(path, None)
        return data
    except OSError:
        return None


def _hls_err(status: int, msg: str):
    return web.Response(status=status, text=msg, headers=_CORS)


async def _hls_serve(req: web.Request, code: str, url: str):
    key = "h" + hashlib.sha1(url.encode()).hexdigest()[:15]
    rng = req.headers.get("Range", "")
    if req.method == "HEAD":
        return web.Response(status=200, headers={**_CORS, "Accept-Ranges": "bytes"}, content_type="video/mp2t")
    data = await asyncio.to_thread(_seg_get, key)
    if data is not None:  # cached segment: straight from disk
        m = re.fullmatch(r"bytes=(\d+)-(\d*)", rng.strip()) if rng else None
        hdr = {**_CORS, "Accept-Ranges": "bytes", "Cache-Control": "no-cache"}
        if m:
            a = int(m.group(1))
            b = min(int(m.group(2)), len(data) - 1) if m.group(2) else len(data) - 1
            if a >= len(data) or b < a:
                return web.Response(status=416, headers={**hdr, "Content-Range": f"bytes */{len(data)}"})
            hdr["Content-Range"] = f"bytes {a}-{b}/{len(data)}"
            return web.Response(status=206, body=data[a:b + 1], headers=hdr, content_type="video/mp2t")
        return web.Response(status=200, body=data, headers=hdr, content_type="video/mp2t")

    h = dict(_proxy_headers)
    if rng:
        h["Range"] = rng
    try:
        up = await _session_getter().get(url, headers=h, allow_redirects=True,
                                         timeout=aiohttp.ClientTimeout(total=None, connect=15, sock_read=30))
    except asyncio.CancelledError:
        raise
    except Exception as e:
        log.warning("HLS upstream failed: %s", e)
        return _hls_err(502, f"Upstream fetch failed: {str(e)[:100]}")
    try:
        if up.status >= 400:
            return _hls_err(502, f"CDN returned HTTP {up.status} (link expired?)")
        it = up.content.iter_chunked(64 * 1024).__aiter__()
        try:
            first = await it.__anext__()
        except StopAsyncIteration:
            first = b""
        if first.lstrip(b"\xef\xbb\xbf \t\r\n").startswith(b"#EXTM3U"):  # a playlist (master / variant): rewrite it
            buf = bytearray(first)
            async for c in it:
                buf += c
                if len(buf) > 5 * 1024 * 1024:
                    return _hls_err(502, "playlist too large")
            text = buf.decode("utf-8", "replace").lstrip("\ufeff")
            return web.Response(text=_rewrite_playlist(text, str(up.url), code), content_type=_HLS_MIME,
                                headers={**_CORS, "Cache-Control": "no-cache"})
        # media segment / key / init section: pipe as it arrives, keep a copy on disk
        clen = up.headers.get("Content-Length", "")
        resp = web.StreamResponse(status=up.status)
        resp.content_type = up.headers.get("Content-Type") or "video/mp2t"
        for k, v in {**_CORS, "Accept-Ranges": "bytes", "Cache-Control": "no-cache"}.items():
            resp.headers[k] = v
        if up.headers.get("Content-Range"):
            resp.headers["Content-Range"] = up.headers["Content-Range"]
        if clen and not up.headers.get("Content-Encoding"):
            resp.headers["Content-Length"] = clen
        await resp.prepare(req)
        want = int(clen) if clen.isdigit() else 0
        tee = bytearray() if (CACHE_ON and not rng and up.status == 200 and 0 < want <= _HLS_MAX_SEG
                              and not up.headers.get("Content-Encoding")) else None
        try:
            if first:
                await resp.write(first)
                if tee is not None:
                    tee += first
            async for c in it:
                await resp.write(c)
                if tee is not None:
                    tee += c
        except (ConnectionResetError, asyncio.CancelledError) as e:
            if isinstance(e, asyncio.CancelledError):
                raise
            return resp  # player gave up / seeked: partial segment is not cached
        except Exception as e:
            log.warning("HLS segment ended early: %s", e)
            if req.transport:
                req.transport.close()
            return resp
        if tee is not None and len(tee) == want:
            await asyncio.to_thread(_cache_put, key, 0, bytes(tee))
        return resp
    finally:
        up.release()


async def _hls_master(req: web.Request):
    code = req.match_info["code"]
    entry = _registry.get(code)
    if not entry or entry.get("kind") != "hls" or time.time() - entry["ts"] > STREAM_TTL:
        return _hls_err(404, "Stream not found (code invalid or expired)")
    return await _hls_serve(req, code, entry["url"])


async def _hls_part(req: web.Request):
    code = req.match_info["code"]
    entry = _registry.get(code)
    if not entry or entry.get("kind") != "hls" or time.time() - entry["ts"] > STREAM_TTL:
        return _hls_err(404, "Stream not found (code invalid or expired)")
    url = _hls_url_from(code, req.match_info["token"])
    if not url:
        return _hls_err(403, "Bad or unsigned HLS token")
    return await _hls_serve(req, code, url)


async def _hls_options(_req):
    return web.Response(status=204, headers=_CORS)


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


def _start_prefetch(entry: dict) -> None:
    if not (CACHE_ON and PREFETCH_ON):
        return
    key = _cache_key(entry)
    if key in _prefetching:
        return
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return
    _prefetching.add(key)
    t = asyncio.create_task(_prefetch(entry, key))
    t.add_done_callback(lambda _t, key=key: (_prefetching.discard(key), _t.cancelled() or _t.exception()))


async def _prefetch(entry: dict, key: str) -> None:
    global _pf_lock
    if _pf_lock is None:
        _pf_lock = asyncio.Lock()
    async with _pf_lock:  # one video at a time
        session, url, base = _session_getter(), entry["url"], dict(_proxy_headers)
        total = _meta.get(key, (0, ""))[0]
        if not total:
            st, h, body = await _fetch_range(session, url, base, 0, PROXY_FIRST - 1)
            total = _total_from(h) if st == 206 else 0
            if not total:
                return
            _meta[key] = (total, h.get("Content-Type", ""))
        limit = int(float(os.getenv("STREAM_PREFETCH_MAX_MB", "0") or 0) * 1048576) or int(CACHE_MAX * 0.6)
        n = (min(total, limit) + CACHE_CHUNK - 1) // CACHE_CHUNK
        log.info("prefetch %s: %d chunks (%.0f MB of %.0f MB)", entry["name"][:40], n, min(total, limit) / 1048576, total / 1048576)
        todo = [i for i in range(n) if not os.path.exists(_cache_path(key, i))]
        for j in range(0, len(todo), PREFETCH_PAR):
            if time.time() - entry["ts"] > STREAM_TTL or _registry.get(hashlib.sha1(entry["url"].encode()).hexdigest()[:12]) is None:
                return  # link expired / dropped
            await asyncio.gather(*(_get_chunk(session, url, base, key, i, total) for i in todo[j:j + PREFETCH_PAR]))
        log.info("prefetch done: %s", entry["name"][:40])


async def _fill_chunk(session, url: str, headers: dict, key: str, idx: int, a: int, b: int) -> bytes:
    data = await _fetch_chunk(session, url, headers, a, b)
    await asyncio.to_thread(_cache_put, key, idx, data)
    return data


async def _get_chunk(session, url: str, headers: dict, key: str, idx: int, total: int) -> bytes:
    """Chunk #idx of the aligned grid: from disk if cached, else downloaded once (shared between simultaneous viewers) and cached."""
    a = idx * CACHE_CHUNK
    b = min(a + CACHE_CHUNK - 1, total - 1)
    data = await asyncio.to_thread(_cache_get, key, idx, b - a + 1)
    if data is not None:
        return data
    k = (key, idx)
    t = _cache_inflight.get(k)
    if t is None:
        t = asyncio.create_task(_fill_chunk(session, url, headers, key, idx, a, b))
        _cache_inflight[k] = t

        def _done(_t, k=k):
            _cache_inflight.pop(k, None)
            if not _t.cancelled():
                _t.exception()  # mark retrieved (the awaiting viewer, if any, still gets it)
        t.add_done_callback(_done)
    return await asyncio.shield(t)  # one viewer leaving must not kill the download another viewer (or the cache) needs


async def _stream_parallel(req: web.Request, entry: dict, rng):
    """Serve the requested range through PROXY_CONN parallel upstream Range requests, in order, via the disk chunk cache.
    Returns None (nothing sent yet) when the upstream does not do ranges / the first fetch fails -> caller falls back to the plain relay."""
    start, end = rng
    has_range = bool(req.headers.get("Range"))
    session, url, base = _session_getter(), entry["url"], dict(_proxy_headers)
    key = _cache_key(entry)
    total, ctype = _meta.get(key, (0, ""))
    first = b""  # small direct piece so playback starts fast when the first chunk is not on disk yet

    async def probe(a: int, b: int):
        try:
            return await _fetch_range(session, url, base, a, b)
        except asyncio.CancelledError:
            raise
        except Exception as e:
            log.info("parallel relay: first piece failed (%s) -> plain relay", e)
            return None

    if not total:  # unknown size / type: ask the CDN for the first piece (this also tells us the total)
        r = await probe(start, start + PROXY_FIRST - 1 if end is None else min(start + PROXY_FIRST - 1, end))
        if r is None:
            return None
        st, h, body = r
        total = _total_from(h) if st == 206 else 0
        if not total or not body:
            log.info("parallel relay: upstream gave HTTP %s / no total -> plain relay", st)
            return None
        ctype = h.get("Content-Type", "")
        _meta[key] = (total, ctype)
        first = body
    if start >= total:
        return web.Response(status=416, headers={"Content-Range": f"bytes */{total}"})
    last = total - 1 if end is None else min(end, total - 1)

    idx0 = start // CACHE_CHUNK
    chunk0_end = min((idx0 + 1) * CACHE_CHUNK - 1, total - 1)
    if not first and not os.path.exists(_cache_path(key, idx0)):  # cold start: grab a small piece right away
        r = await probe(start, min(start + PROXY_FIRST - 1, chunk0_end, last))
        if r is None:
            return None
        st, h, body = r
        if st != 206 or not body:
            return None
        first = body
    first = first[: min(chunk0_end, last) - start + 1]  # never past the first chunk / the requested end

    name = entry["name"]
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
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

    idx_last = last // CACHE_CHUNK
    pending: deque = deque()  # (idx, task)
    nxt = idx0

    def schedule():
        nonlocal nxt
        while nxt <= idx_last and len(pending) < PROXY_CONN:
            pending.append((nxt, asyncio.create_task(_get_chunk(session, url, base, key, nxt, total))))
            nxt += 1

    try:
        schedule()  # chunks download (or load from disk) while the first piece is being written
        pos = start
        if first:
            await resp.write(first)
            pos += len(first)
        while pending:
            idx, task = pending.popleft()
            data = await task
            schedule()  # keep the window full
            a = idx * CACHE_CHUNK
            lo, hi = max(pos, a) - a, min(last, a + len(data) - 1) - a + 1
            if hi > lo:
                await resp.write(data[lo:hi])
                pos = a + hi
    except ConnectionResetError:  # player closed / seeked: normal
        log.debug("parallel relay: client disconnected")
    except asyncio.CancelledError:
        raise
    except Exception as e:  # CDN kept failing mid-stream: cut the connection so the player retries
        log.warning("parallel relay aborted: %s", e)
        if req.transport:
            req.transport.close()
    finally:
        for _i, t in pending:
            t.cancel()
        if pending:
            await asyncio.gather(*(t for _i, t in pending), return_exceptions=True)
    return resp


def _run_end(key: str, idx: int, idx_last: int, total: int, last: int) -> int:
    """Last byte of the run of consecutive NOT-cached chunks starting at chunk idx (one upstream request covers the whole run)."""
    j = idx
    while j < idx_last and not os.path.exists(_cache_path(key, j + 1)):
        j += 1
    return min((j + 1) * CACHE_CHUNK - 1, total - 1, last)


async def _open_up(session, url: str, base: dict, a: int, b):
    h = dict(base)
    h["Range"] = f"bytes={a}-{b}"
    return await session.get(url, headers=h, allow_redirects=True,
                             timeout=aiohttp.ClientTimeout(total=None, connect=15, sock_read=30))


async def _stream_pipe(req: web.Request, entry: dict, rng):
    """Pipe the CDN to the player as bytes arrive (like a plain relay) while copying every complete cache-grid chunk to disk.
    Chunks already on disk are served from disk. Returns None (nothing sent yet) when the CDN does not do ranges -> plain relay."""
    start, end = rng
    has_range = bool(req.headers.get("Range"))
    session, url, base = _session_getter(), entry["url"], dict(_proxy_headers)
    key, C = _cache_key(entry), CACHE_CHUNK
    total, ctype = _meta.get(key, (0, ""))
    up, up_end, pre, idx0 = None, 0, None, start // C
    try:
        if not total:  # unknown size: the first CDN answer tells us
            up = await _open_up(session, url, base, start, "" if end is None else end)
            total = _total_from(up.headers) if up.status == 206 else 0
            if not total:
                log.info("pipe relay: CDN gave HTTP %s / no total -> plain relay", up.status)
                up.release()
                return None
            ctype = up.headers.get("Content-Type", "")
            _meta[key] = (total, ctype)
        if start >= total:
            if up:
                up.release()
            return web.Response(status=416, headers={"Content-Range": f"bytes */{total}"})
        last = total - 1 if end is None else min(end, total - 1)
        idx_last = last // C
        if up is not None:
            up_end = last
        else:
            a0 = idx0 * C
            pre = await asyncio.to_thread(_cache_get, key, idx0, min(a0 + C - 1, total - 1) - a0 + 1) if CACHE_ON else None
            if pre is None:  # not on disk: open the CDN before answering the player
                up_end = _run_end(key, idx0, idx_last, total, last)
                up = await _open_up(session, url, base, start, up_end)
                if up.status != 206:
                    log.info("pipe relay: CDN gave HTTP %s -> plain relay", up.status)
                    up.release()
                    return None
    except asyncio.CancelledError:
        if up:
            up.release()
        raise
    except Exception as e:
        log.info("pipe relay: first request failed (%s) -> plain relay", e)
        if up:
            up.release()
        return None

    name = entry["name"]
    ext = name.rsplit(".", 1)[-1].lower() if "." in name else ""
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

    puts: set = set()  # strong refs to in-flight disk writes

    def save(ci: int, data: bytes):
        t = asyncio.create_task(asyncio.to_thread(_cache_put, key, ci, data))
        puts.add(t)
        t.add_done_callback(puts.discard)

    pos, cbuf = start, None
    try:
        while pos <= last:
            idx = pos // C
            a, b = idx * C, min(idx * C + C - 1, total - 1)
            if up is None:
                data = pre if (pre is not None and idx == idx0) else (
                    await asyncio.to_thread(_cache_get, key, idx, b - a + 1) if CACHE_ON else None)
                pre = None
                if data is not None:  # cache hit: straight from disk
                    hi = min(last, b) - a + 1
                    await resp.write(data[pos - a:hi])
                    pos = a + hi
                    continue
                up_end = _run_end(key, idx, idx_last, total, last)
                up = await _open_up(session, url, base, pos, up_end)
                if up.status != 206:
                    raise RuntimeError(f"CDN HTTP {up.status}")
            off, cbuf = pos, None
            async for piece in up.content.iter_chunked(64 * 1024):
                if off + len(piece) - 1 > up_end:
                    piece = piece[: up_end - off + 1]
                await resp.write(piece)  # to the player first: no waiting for the disk
                if CACHE_ON:
                    mv, o = memoryview(piece), off
                    while len(mv):
                        ci = o // C
                        ca, cb = ci * C, min(ci * C + C - 1, total - 1)
                        take = min(len(mv), cb - o + 1)
                        if o == ca:
                            cbuf = bytearray()  # only chunks we see from their first byte are cached
                        if cbuf is not None:
                            cbuf += mv[:take]
                            if o + take - 1 == cb:
                                if len(cbuf) == cb - ca + 1:
                                    save(ci, bytes(cbuf))
                                cbuf = None
                        o += take
                        mv = mv[take:]
                off += len(piece)
                if off > up_end:
                    break
            up.release()
            up = None
            if off <= up_end:
                raise RuntimeError(f"CDN closed early at {off} of {up_end}")
            pos = off
    except ConnectionResetError:  # player closed / seeked: normal
        log.debug("pipe relay: client disconnected at %d", pos)
    except asyncio.CancelledError:
        raise
    except Exception as e:  # CDN failed mid-stream: cut the connection so the player retries
        log.warning("pipe relay aborted: %s", e)
        if req.transport:
            req.transport.close()
    finally:
        if up is not None:
            up.release()
    return resp


async def _stream(req: web.Request):
    code = req.match_info["code"]
    entry = _registry.get(code)
    if entry and entry.get("kind") == "hls":
        return web.Response(status=404, text="Use the /hls/ link for this stream")
    browser = req.method == "GET" and "text/html" in req.headers.get("Accept", "").lower()
    if (entry and PROXY_MODE != "relay" and not req.query.get("relay") and not browser and not _has_cache(entry)
            and time.time() - entry["ts"] <= STREAM_TTL and str(entry["url"]).startswith(("http://", "https://"))):
        # VLC / MX Player / ExoPlayer / HEAD ...: they ignore Content-Disposition and do their Range requests against the CDN
        return web.Response(status=302, headers={"Location": entry["url"], "Cache-Control": "no-store",
                                                 "Access-Control-Allow-Origin": "*"})
    if entry:
        _start_prefetch(entry)
    if entry and req.method == "GET" and time.time() - entry["ts"] <= STREAM_TTL and (PIPE_ON or PROXY_CONN > 1):
        rng = _parse_range(req.headers.get("Range"))
        if rng is not None:
            resp = await (_stream_pipe if PIPE_ON else _stream_parallel)(req, entry, rng)
            if resp is not None:
                return resp
    return await _stream_simple(req)  # relay: HEAD, suffix ranges, upstream without Range support, unknown code (404) ...


async def start_web_server(port: int, session_getter, proxy_headers: dict) -> web.AppRunner:
    global _session_getter, _proxy_headers
    _session_getter, _proxy_headers = session_getter, dict(proxy_headers)
    _cache_init()
    app = web.Application()
    app.router.add_get("/", _root)
    app.router.add_get("/health", _health)
    app.router.add_get("/stream/{code}", _stream)
    app.router.add_get("/hls/{code}/master.m3u8", _hls_master)
    app.router.add_get("/hls/{code}/p/{token}", _hls_part)
    app.router.add_route("OPTIONS", "/hls/{path:.*}", _hls_options)
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
