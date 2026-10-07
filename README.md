# 🚀 TeraBox Downloader Bot

> ⚡ **Powerful • Fast • Reliable • Developer Edition**

A professional **Telegram TeraBox Downloader Bot** built with **Pyrogram / Kurigram**. Send a TeraBox share link and get **Telegram files, Stream Links, Direct Links & M3U8** through a fast and polished workflow.

---

## ✨ Features at a Glance

- 🔎 Smart TeraBox link detection
- 📥 Download files directly to Telegram
- 🎬 Streamable video support
- 🔗 Direct download links
- 📺 M3U8 / HLS support
- ⚡ Parallel high-speed downloads
- 🖼️ Sharp automatic video thumbnails
- 📊 Live progress, speed & ETA
- 📁 Folder and multi-file support
- 🛡️ Multiple fallback resolvers
- 💎 Premium plans with bonus (extra) days
- ♻️ Auto-resume of interrupted downloads after a restart
- 👑 Admin controls
- 💾 JSON storage with optional MongoDB
- 🐳 Docker-ready deployment

---

# 🛠️ Installation

## 1️⃣ Configure Environment

Copy the example environment file:

```bash
cp .env.example .env
```

Configure your credentials:

```env
API_ID=
API_HASH=
BOT_TOKEN=
OWNER_ID=
```

Optional configuration may include:

```env
ADMINS=
LOG_CHANNEL=
MONGO_URI=
PARALLEL_CONNECTIONS=8
RANK_FIRST=0
STREAM_DEADLINE=12
SPLIT_LARGE=1
```

## 2️⃣ Install Dependencies

```bash
pip install -r requirements.txt
```

## 3️⃣ Start the Bot

```bash
python bot.py
```

---

# 🐳 Docker Deployment

Build the image:

```bash
docker build -t terabox-bot .
```

Run the container:

```bash
docker run --env-file .env terabox-bot
```

---

# 🔎 TeraBox Link Detection

The bot supports major TeraBox mirrors and compatible clones, including:

`terabox` • `teraboxapp` • `1024terabox` • `terafileshare` • `4funbox` • `mirrobox` • `nephobox` • `momerybox` • `tibibox` • `dubox` • and other compatible `tera*` share-style domains.

### 🔗 Link Normalization

The bot can normalize:

- 🌐 Scheme-less links
- 🔗 Standard share links
- 📌 `?surl=` sharing links
- 📁 Share-style paths

---

# 📁 Folder Support

Shared folders can be scanned automatically.

Example folder link:

```text
https://1024terabox.com/s/1jY8HXLPMuoRekOw_Gqr81Q
```

### Available actions

- ◀️ Previous page
- ▶️ Next page
- 📄 10 files per page
- 📥 Select individual files
- 📦 Download all
- ❌ Cancel the batch

---

# 🎛️ Download Menu

Users can choose from:

| Button | Function |
|---|---|
| 📥 Download | Send the file to Telegram |
| 🎬 Stream | Generate a playable stream |
| 🔗 Direct Link | Return the direct file URL |
| 📺 M3U8 | Return/use an HLS playlist |

---

# 🖼️ Smart Thumbnail System

The bot creates a sharp video preview whenever possible.

### Thumbnail priority

```text
🎬 FFmpeg video frame
        ↓
🖼️ TeraBox API thumbnail
        ↓
📝 Text-only information card
```

For videos, a frame is extracted around **10% of the video's duration**. The same area is used for the sent video's thumbnail when available.

This avoids the usual text-first/photo-later flicker and keeps the preview visually consistent.

---

# 📊 Live Download & Upload Progress

The bot provides live progress information:

```text
📥 Downloading...
━━━━━━━━━━━━━━━━
📊 Progress
⚡ Speed
⏱️ ETA
```

### Controls

- ⚡ Real-time speed
- 📊 Progress percentage
- ⏱️ Estimated time remaining
- ❌ Cancel button
- `/cancel` command

After a successful upload, the temporary progress/menu message is removed so the chat stays clean.

---

# 🎬 File Handling

### Videos

🎥 Uploaded as streamable Telegram videos when supported.

### Other Files

📄 Uploaded as Telegram documents.

### Large Files

🔗 Files above the configured Telegram upload limit are handled through links/splitting according to the active configuration.

---

# 🔄 Fallback Resolver System

If the primary **PlayTeraBox API** fails, the bot can try additional resolvers concurrently.

```text
                 ┌─ FlowVideoPlayer
                 ├─ Azhawasadda
PlayTeraBox ────┼─ AnshAPI
                 └─ Baidu PCS
```

🏆 The first successful resolver can be used for supported single-file operations.

> ℹ️ Folder listing is not available through every fallback resolver.

---

# 🎬 Advanced Stream System

The **Stream** button can race the current resolver and fallback links.

The process is:

1. 🔎 Collect playable links
2. ⚡ Check available servers
3. 📊 Test response speed
4. 🏆 Select the fastest suitable link
5. 🎬 Serve it through the bot's seekable stream proxy (single 🔗 Open Stream button)

### Stream timeout

```env
STREAM_DEADLINE=12
```

The card shows **one** `🔗 Open Stream` button (the bot's `/stream/<code>` link, so it plays inline and seeks instead of downloading). The API's HLS/M3U8 link is used only when no normal playable link is available.

---

# 💎 Premium Plans

Every paid plan comes with **bonus (extra) days**. The plans list, the plan buttons and the payment message all show them.

| 💰 Price | 📅 Plan | 🎁 Total days granted |
|---|---|---:|
| ₹99 | 15 Days + 3 Extra Days | 18 |
| ₹199 | 30 Days + 5 Extra Days | 35 |
| ₹299 | 45 Days + 5 Extra Days | 50 |
| ₹399 | 60 Days + 8 Extra Days | 68 |
| ₹599 | 90 Days + 10 Extra Days | 100 |
| ₹999 | 150 Days + 15 Extra Days | 165 |

Change the plans with the `PLANS` env variable. Format is `price:days+extra` (use `price:lifetime` for a forever plan):

```env
PLANS=99:15+3,199:30+5,299:45+5,399:60+8,599:90+10,999:150+15
```

> ⚠️ If `PLANS` is already set on your host (e.g. Render), it overrides the defaults in the code. Update or remove it there.

Parallel downloads per plan come from `PARALLEL_LIMITS` (`days:limit`, `0` = unlimited), matched against the **total** days granted:

| 💰 Plan | ⚡ Parallel downloads |
|---|---:|
| ₹99 | 1 |
| ₹199 | 3 |
| ₹299 | 5 |
| ₹399 | 10 |
| ₹599 | 12 |
| ₹999 | 15 |


```env
PARALLEL_LIMITS=free:1,18:1,35:3,50:5,68:10,100:12,165:15,lifetime:0
```

---

# ♻️ Auto-Resume After Restart

If the bot stops in the middle of a download (restart, redeploy, crash), it **continues from where the progress bar stopped** once it is back online.

- 📍 Progress of every download connection is saved next to the partial file
- 🔄 On startup the bot gets a fresh link for the file and continues into the same partial file (the progress message is edited, a new one is sent if it is gone)
- ✅ If the file was already fully downloaded (bot stopped while uploading), it goes straight to upload
- 🛑 The user's **Cancel** button still deletes everything
- 🔁 A download is retried at most `RESUME_MAX_TRIES` times and dropped after `RESUME_MAX_AGE_HOURS`

```env
RESUME_DOWNLOADS=1
RESUME_MAX_AGE_HOURS=12
RESUME_MAX_TRIES=3
DOWNLOAD_DIR=downloads
```

> ⚠️ Partial files live in `DOWNLOAD_DIR`. On hosts with an **ephemeral disk** (Render free plan) the disk is wiped on every restart, so the download restarts from 0% (it still restarts automatically). To really continue from the old percentage, mount a **persistent disk / volume** at `DOWNLOAD_DIR` (and keep `bot_data.json` there too, or use `MONGO_URI`).
>
> 📁 For "Download all" on a folder, only the file that was running is resumed; send the folder link again for the remaining files.

---

# ⚡ Fast Parallel Downloads

For larger files, the bot can use multiple ranged connections.

Default:

```env
PARALLEL_CONNECTIONS=8
```

Files of **20 MB+** can use parallel ranged connections.

### If a connection fails

The bot can:

1. 🔄 Retry from the interrupted position
2. ⬇️ Continue downloading
3. 🛡️ Fall back to a normal single connection when Range support is unavailable

Disable parallel downloading:

```env
PARALLEL_CONNECTIONS=1
```

---

# 🔁 Automatic Link Switching

If a download link:

- 🐌 becomes too slow
- ❌ returns an error page
- 💥 fails during download

the bot can request a fresh link from the fallback resolvers and retry once for supported single-file shares.

---

# 🧪 Fake File Protection

Before upload, the bot checks downloaded content.

It rejects:

- ❌ HTML error pages
- ❌ JSON error responses
- ❌ Invalid files
- ❌ Suspicious/tiny video responses

This helps prevent broken API responses from being uploaded as files.

---

# 🧩 Automatic Extension Fix

If a downloaded file does not have a usable extension, the bot can detect its file type from its **magic bytes** and assign an appropriate extension.

---

# 🎥 Video Metadata

Supported videos can include:

- 🎬 Duration
- 📦 File size
- 🖼️ Sharp thumbnail
- 📹 Streamable video output

FFmpeg/FFprobe are required for the related media-processing features.

---

# 📦 Large File Splitting

Large files can be split according to configuration.

Default values:

```text
MAX_FILE_SIZE_MB = 2000
MAX_SPLIT_MB     = 8192
SPLIT_LARGE      = 1
```

### Video files

🎬 Split using FFmpeg stream-copy where supported.

### Other files

📄 Split into:

```text
file.001
file.002
file.003
...
```

> ⚠️ Splitting large files may require roughly **2× the original file size** in available disk space.

Disable large-file splitting:

```env
SPLIT_LARGE=0
```

---

# 👑 Admin System

Included administrative commands:

```text
/stats
/broadcast
/ban
/unban
```

## 🚫 Ban / Unban

Supported formats:

```text
/ban <id>
/ban @username
```

You can also reply to a user's message with:

```text
/ban
/unban
```

The bot can handle supported cases involving:

- 💬 Group messages
- ↪️ Forwarded messages
- 🤖 Bot messages containing `ID: <number>`
- 👤 Known usernames

🛡️ Administrators cannot be banned.

### 👑 Admin / Owner privileges

- ♾️ Unlimited parallel downloads
- 🚀 Never wait in the download queue (no "⏳ Queued…" message)
- 💎 Unlimited access without a premium plan

---

# 🛡️ User & Access Controls

The bot supports:

- 🔐 Force subscription
- 📢 Log channel
- 📅 Daily limits
- 👤 Per-user concurrency limits
- 🌐 Global concurrency limits

---

# 💾 Storage

## Default Storage

```text
📄 JSON
```

## Optional Database

```text
🍃 MongoDB
```

Set MongoDB through:

```env
MONGO_URI=
```

Environment variables override hardcoded defaults where supported by the bot configuration.

> 🔐 Keep `API_ID`, `API_HASH`, `BOT_TOKEN`, owner/admin IDs, log-channel settings and database credentials private. Never publish real secrets in a public repository.

---

# ⚙️ Configuration

| ⚙️ Variable | 🔢 Default | 📝 Purpose |
|---|---:|---|
| `PARALLEL_CONNECTIONS` | `8` | Ranged connections for files 20 MB+ |
| `RANK_FIRST` | `0` | Speed-test servers before downloading |
| `FRAME_TIMEOUT` | `20s` | Maximum thumbnail generation wait |
| `STREAM_DEADLINE` | `12s` | Stream resolver deadline |
| `PLANS` | `99:15+3,…,999:150+15` | Premium plans (`price:days+extra`) |
| `PARALLEL_LIMITS` | `free:1,18:1,35:3,50:5,68:10,100:12,165:15,lifetime:0` | Parallel downloads per plan |
| `RESUME_DOWNLOADS` | `1` | Continue interrupted downloads after a restart |
| `RESUME_MAX_AGE_HOURS` | `12` | Drop interrupted downloads older than this |
| `RESUME_MAX_TRIES` | `3` | Max resume attempts per download |
| `MAX_CONCURRENT_DOWNLOADS` | `0` | Global download slots for normal users (`0` = no limit). Admin/owner skip the queue |
| `MAX_FILE_SIZE_MB` | `2000` | Large-file threshold |
| `MAX_SPLIT_MB` | `8192` | Maximum split size |
| `SPLIT_LARGE` | `1` | Enable large-file splitting |

---

# 🏎️ Server Ranking

### Maximum initial optimization

```env
RANK_FIRST=1
```

The bot speed-tests available servers before starting the download and attempts to choose the best server.

### Fastest start

```env
RANK_FIRST=0
```

The bot starts immediately and switches only when the current link becomes slow or fails.

---

# 🧰 Tech Stack

```text
🐍 Python
🤖 Pyrogram / Kurigram
🎬 FFmpeg / FFprobe
🌐 REST APIs
💾 JSON / MongoDB
🐳 Docker
⚡ Async Processing
```

---

# 📂 Project Structure

A typical deployment can contain:

```text
.
├── bot.py
├── requirements.txt
├── .env.example
├── Dockerfile
├── README.md
└── other project modules
```

> ℹ️ The exact repository structure may vary depending on the version of the project.

---

# 🚀 Quick Start

```bash
git clone <YOUR_REPOSITORY_URL>
cd <YOUR_PROJECT_DIRECTORY>

cp .env.example .env
nano .env

pip install -r requirements.txt
python bot.py
```

### Docker

```bash
docker build -t terabox-bot .
docker run --env-file .env terabox-bot
```

---

# 🏆 Why This Bot?

| 💎 Feature | Status |
|---|---|
| ⚡ Fast Downloads | ✅ |
| 📁 Folder Support | ✅ |
| 🎬 Video Streaming | ✅ |
| 💎 Premium Plans + Extra Days | ✅ |
| 🔗 Direct Links | ✅ |
| 📺 M3U8 Support | ✅ |
| 🖼️ Smart Thumbnails | ✅ |
| 📊 Live Progress | ✅ |
| 🔄 Fallback APIs | ✅ |
| 🛡️ Link Switching | ✅ |
| 📦 Large File Handling | ✅ |
| ♻️ Resume After Restart | ✅ |
| 👑 Admin Controls | ✅ |
| 💾 MongoDB Support | ✅ |
| 🐳 Docker Support | ✅ |

---

# 🧑‍💻 Developer

## 👨‍💻 Anuj Kumar

**Professional Telegram Bot Developer**

> 💎 Building powerful, fast & reliable automation systems.

### ❤️ Developer Philosophy

**Code clean. Build smart. Deploy reliably.**

---

# ⭐ Support the Project

If this project helps you:

⭐ **Star the repository**  
🍴 **Fork the project**  
🐛 **Report bugs**  
💡 **Suggest improvements**  
📢 **Share the project**

---

# ⚠️ Disclaimer

This project is intended for **educational and development purposes**.

Users are responsible for the content they download and for complying with applicable laws, platform terms, copyright requirements, and API usage policies.

The developer is not responsible for misuse of the software.

---

# 📜 License

Use and distribute this project according to the license included with the repository.

---

<div align="center">

### 🚀 TeraBox Downloader Bot

**Fast • Smart • Reliable • Professional**

**Made with ❤️ & ☕ by Anuj Kumar**

</div>
