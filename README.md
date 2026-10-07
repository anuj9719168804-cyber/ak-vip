# Terabox Downloader Bot

Telegram bot (Pyrogram/kurigram) that takes a Terabox share link and returns the file, a stream link and a direct link, using the playterabox API.

## Run
```bash
cp .env.example .env   # fill API_ID, API_HASH, BOT_TOKEN (+ OWNER_ID)
pip install -r requirements.txt
python bot.py
```
Docker: `docker build -t terabox-bot . && docker run --env-file .env terabox-bot`

## Features
- Terabox link detection for all known mirrors (terabox, teraboxapp, 1024terabox, terafileshare, 4funbox, mirrobox, nephobox, momerybox, tibibox, dubox, ...) plus unknown "tera*" clones with share-style paths; scheme-less links and wap/sharing ?surl= links are normalised to /s/1<id>
- Folder shares: sub-folders are scanned, files listed 10 per page (◀️ Prev / Next ▶️), pick one or "Download all" (sent one by one, Cancel stops the batch)
- Buttons: Download to Telegram, Stream, Direct Link, M3U8
- Live download/upload progress with speed + ETA, Cancel button and /cancel
- Videos are sent as streamable videos, everything else as documents; files above 2 GB get links only
- Force-subscribe, log channel, daily limit, per-user and global concurrency limits
- Admin: /stats, /broadcast (reply to a message), /ban <id>, /unban <id>
- JSON storage by default, MongoDB if MONGO_URI is set

Credentials (API_ID/API_HASH/BOT_TOKEN, OWNER_ID/ADMINS, LOG_CHANNEL, MONGO_URI) are hardcoded as defaults in `bot.py` (copied from fbot); a real environment variable still overrides them.

## Fallbacks
If the primary playterabox API fails, the bot tries `flowvideoplayer.com`, then `azhawasadda.in`, then `terabox.beer` (single-file links only, no folder listing). `terabox.beer` often returns an HLS (.m3u8) stream, which is converted to mp4 with **ffmpeg** (included in the Docker image; install it yourself when running without Docker).

## Fast downloads
Files of 20 MB+ are split over `PARALLEL_CONNECTIONS` (default 8) ranged connections and written into one file. A broken connection is retried from where it stopped; if the server has no Range support (or keeps failing) the bot silently falls back to a normal single-connection download. Set `PARALLEL_CONNECTIONS=1` to turn it off.

## Reliability extras
- **Link switching:** if the download link is slow (<150 KB/s after 12 s), returns an error page, or fails, the bot automatically gets a fresh link from the fallback resolvers and retries once (single-file shares).
- **Fake-file check:** HTML/JSON error pages and tiny "videos" are rejected instead of being uploaded.
- **Extension fix:** files without an extension get one from their magic bytes.
- **Video metadata:** duration/size are sent with videos, and a thumbnail is cut from the video when the API gives none (needs ffprobe/ffmpeg).
- **Big files:** above `MAX_FILE_SIZE_MB` (2000) and up to `MAX_SPLIT_MB` (8192) files are split — videos with ffmpeg stream-copy into playable parts, other files into raw `.001/.002…` parts. Disk needs roughly 2x the file size while splitting. `SPLIT_LARGE=0` turns it off.
