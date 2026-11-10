# GoondVR

A self-hosted web UI and CLI for recording public livestreams from Chaturbate and Stripchat. Supports macOS, Windows, Linux, and Docker. Favicon from [Twemoji](https://github.com/twitter/twemoji).

> [!IMPORTANT]
> The original `teacat/chaturbate-dvr` repository is archived and no longer maintained. This fork remains active and includes ongoing fixes, new releases, and additional features like Stripchat support.

Features added in this version include:

- Updated web UI
- Live stream thumbnails for streaming channels and previews for offline channels
- Stripchat support
- Discord webhook and `ntfy` notifications

Example dashboard and settings views from the current web UI.

![Dashboard](docs/screenshots/dashboard.png)

![Dashboard Grid](docs/screenshots/dashboard-grid.png)

![Dashboard Compact](docs/screenshots/dashboard-compact.png)

![Add Channel](docs/screenshots/addchannel.png)

![Settings](docs/screenshots/settings.png)

# Getting Started

Go to the [📦 Releases page](https://github.com/HeapOfChaos/goondvr/releases) and download the appropriate binary. (e.g., `windows_amd64_goondvr.exe`)

## 🌐 Launching the Web UI

```bash
# Windows
$ windows_amd64_goondvr.exe

# macOS (Intel)
$ ./darwin_amd64_goondvr

# macOS (Apple Silicon)
$ ./darwin_arm64_goondvr

# Linux
$ ./linux_amd64_goondvr
```

Then visit [`http://localhost:8080`](http://localhost:8080) in your browser.

## 💻 Using as a CLI Tool

```bash
# Windows
$ windows_amd64_goondvr.exe -u CHANNEL_USERNAME --site chaturbate

# macOS (Intel)
$ ./darwin_amd64_goondvr -u CHANNEL_USERNAME --site chaturbate

# macOS (Apple Silicon)
$ ./darwin_arm64_goondvr -u CHANNEL_USERNAME --site chaturbate

# Linux
$ ./linux_amd64_goondvr -u CHANNEL_USERNAME --site chaturbate
```

This starts recording immediately. The Web UI will be disabled.

## 🐳 Running with Docker

Pre-built image from [GitHub Container Registry](https://github.com/HeapOfChaos/goondvr/pkgs/container/goondvr):

Persist `./videos` for recordings and `./conf` for saved channels and settings.

By default, closed recordings are moved into a `completed` subdirectory under the recording directory after they are finalized.
The Docker image built from this repository includes `ffmpeg` so remux/transcode mode works out of the box there.

```bash
# Run the container and save videos to ./videos
$ docker run -d \
    --name my-dvr \
    --restart unless-stopped \
    -p 8080:8080 \
    -v "./videos:/usr/src/app/videos" \
    -v "./conf:/usr/src/app/conf" \
    ghcr.io/heapofchaos/goondvr:latest
```

...Or build your own image using the Dockerfile in this repository.

```bash
# Build the image
$ docker build -t goondvr .

# Run the container and save videos to ./videos
$ docker run -d \
    --name my-dvr \
    --restart unless-stopped \
    -p 8080:8080 \
    -v "./videos:/usr/src/app/videos" \
    -v "./conf:/usr/src/app/conf" \
    goondvr
```

...Or use [`docker-compose.yml`](https://github.com/HeapOfChaos/goondvr/blob/master/docker-compose.yml):

```bash
$ docker compose up -d
```

Then visit [`http://localhost:8080`](http://localhost:8080) in your browser.

# 🧾 Command-Line Options

Available options:

```
--username value, -u value  The username of the channel to record
--site value                Site to record from: chaturbate or stripchat (default: "chaturbate")
--admin-username value      Username for web authentication (optional)
--admin-password value      Password for web authentication (optional)
--framerate value           Desired framerate (FPS) (default: 30)
--resolution value          Desired resolution (e.g., 1080 for 1080p) (default: 1080)
--pattern value             Template for naming recorded videos (default: "videos/{{if ne .Site \"chaturbate\"}}{{.Site}}/{{end}}{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}")
--max-duration value        Split video into segments every N minutes ('0' to disable) (default: 0)
--max-filesize value        Split video into segments every N MB ('0' to disable) (default: 0)
--port value, -p value      Port for the web interface and API (default: "8080")
--interval value            Check if the channel is online every N minutes (default: 1)
--cookies value             Cookies to use in the request (format: key=value; key2=value2)
--user-agent value          Custom User-Agent for the request
--browser-mode value        Browser-backed fallback for Chaturbate and Stripchat: off, local, or remote (default: "off")
--browser-path value        Browser executable for local/helper browser mode (default: "chromium")
--browser-profile-dir value Persistent browser profile directory for browser mode (default: "./conf/browser-profile")
--browser-helper-url value  Remote browser helper base URL, used when browser-mode=remote
--browser-helper-token value
                            Bearer token for remote browser helper authentication
--browser-helper            Run browser helper server instead of the DVR app
--browser-helper-bind value Bind address for browser helper server (default: "127.0.0.1:8091")
--browser-debug-port value  Chromium remote debugging port for the persistent browser session (default: 9222)
--browser-bootstrap         Open a visible Chromium window for helper-mode Cloudflare validation
--browser-bootstrap-url value
                            Initial URL to open for browser bootstrap (default: "https://chaturbate.com/")
--stripchat-pdkey value     Manually specify a legacy Stripchat pdkey fallback when auto/browser detection fails
--domain value              Chaturbate domain to use (default: "https://chaturbate.com/")
--completed-dir value       Directory to move fully closed recordings into (default: <recording dir>/completed)
--finalize-mode value       Post-process closed recordings: none, remux, or transcode (default: "none")
--ffmpeg-encoder value      FFmpeg video encoder for transcode mode (default: "libx264")
--ffmpeg-container value    FFmpeg output container for remux/transcode mode: mp4 or mkv (default: "mp4")
--ffmpeg-quality value      FFmpeg quality value; CRF for software encoders, CQ/global quality for many hardware encoders (default: 23)
--ffmpeg-preset value       FFmpeg preset for transcode mode (default: "medium")
--debug                     Dump full HTML to a temp file when stream detection fails, for diagnosing Cloudflare blocks
--help, -h                  show help
--version, -v               print the version
```

**Examples**:

```bash
# Record at 720p / 60fps
$ ./goondvr -u yamiodymel -resolution 720 -framerate 60

# Record from Stripchat
$ ./goondvr -u some_model --site stripchat

# Split every 30 minutes
$ ./goondvr -u yamiodymel -max-duration 30

# Split at 1024 MB
$ ./goondvr -u yamiodymel -max-filesize 1024

# Custom filename format
$ ./goondvr -u yamiodymel \
    -pattern "video/{{.Username}}/{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}_{{.Sequence}}"

# Move closed recordings to another disk
$ ./goondvr -u yamiodymel \
    -completed-dir "/mnt/archive/goondvr-completed"

# Remux finalized recordings with ffmpeg for better seekability
$ ./goondvr -u yamiodymel \
    -finalize-mode remux

# Transcode finalized recordings to save space
$ ./goondvr -u yamiodymel \
    -finalize-mode transcode \
    -ffmpeg-encoder libx264 \
    -ffmpeg-quality 23 \
    -ffmpeg-preset medium
```

_Note: In Web UI mode, these flags serve as default values for new channels._

When the app is stopped with `Ctrl+C` or the container shuts down, it now waits for active recordings to close and for any finalization moves to finish before exiting.

For `remux` and `transcode`, `ffmpeg` must be installed and available on `PATH`. If you choose a hardware encoder such as `h264_nvenc`, `hevc_nvenc`, `h264_qsv`, or `h264_vaapi`, the host and container also need compatible GPU/device access. If FFmpeg or the selected encoder is unavailable, the app keeps the original recording instead of deleting it.

Common encoder examples: `libx264`, `libx265`, `h264_nvenc`, `hevc_nvenc`, `h264_qsv`, `h264_vaapi`.

To prevent file collisions, channels whose filename patterns would resolve to the same output path cannot be added or loaded at the same time. If you record the same username on multiple sites, make sure the pattern includes `{{.Site}}` or otherwise produces distinct paths.

On startup, the app will automatically migrate the old default filename pattern to the newer site-aware default when that is enough to resolve a cross-site filename collision. Custom conflicting patterns are not rewritten automatically and will still need manual adjustment.

# 🍪 Cookies & User-Agent

You can set Cookies and User-Agent via the Web UI or command-line arguments.

![localhost_8080_ (4)](https://github.com/user-attachments/assets/cbd859a9-4255-404b-b6bf-fa89342f7258)

_Note: You can paste either a semicolon-separated cookie string, e.g. `key1=value1; key2=value2`, or a full browser `Cookie:` header. The value is persisted in `conf/settings.json` and can be updated later from the Web UI._

On first start, the app creates local placeholder files in `conf/settings.json` and `conf/channels.json` automatically if they do not exist. Those files are intended to stay local and hold your machine-specific settings and cookies. In this repository, `conf/` is gitignored, so your local cookies and channels are not included in normal commits.

**What is sensitive:** everything in `conf/` stays on your machine: cookies, `admin-password`, the ntfy token, the Discord webhook, the browser-helper token, the browser profile (`conf/browser-profile`) and the affiliate ID saved as `presence_campaign`. GoondVR does not read a `.env` file or environment variables (it is configured with command-line flags and `conf/settings.json`), so no `.env.example` is needed; if you add a `.env` for your own tooling it is gitignored, and only a `.env.example` with variable names and empty values should be committed.

The easiest update path in the Web UI is the `Browser Import` box: paste Firefox `Copy as cURL` output for a successful `chatvideocontext` or `biocontext` request, and the app will extract the cookie jar and User-Agent automatically.

Room-specific `tbu_*` cookies are ignored automatically and are not needed for normal Chaturbate recording.

If copied cookies stop working, enable `Browser Fallback` in Settings. The app can either:

- use a persistent local Chromium profile on the same machine
- call a remote browser helper running on another machine, such as a desktop Ubuntu VM

For Chaturbate, browser fallback is mainly a Cloudflare recovery path when copied cookies and User-Agent stop working.

For Stripchat, the browser-assisted path is now the more reliable recovery path when the direct API or player-derived keys change. Normal key rotation should not require a new app release as long as the helper can still observe the real playlist and segment requests.

## Recommended Quick Start

For most Chaturbate setups, this is the simplest order of operations:

1. Start with the Web UI `Browser Import` box and paste a fresh Firefox `Copy as cURL` request for `chatvideocontext` or `biocontext`.
2. If copied cookies and User-Agent stop working, enable `Browser Fallback`.
3. Use `local` browser mode when the DVR and Chromium profile are on the same machine.
4. Use `remote` browser mode when the DVR is headless and a separate desktop or VM can run Chromium.
5. Keep `Cookies` and `User-Agent` in Settings as the manual override path when you need to inspect or replace values directly.

## Stripchat Notes

- Stripchat support now prefers automatic browser-observed playlist and segment discovery when the old API path breaks.
- `--stripchat-pdkey` is kept as a manual compatibility fallback, but it is no longer the primary fix for ordinary key rotation.
- If Stripchat breaks again, the first thing to check is whether the configured local or remote browser helper can still open the room page and observe live media requests.

## ☁️ Bypass Cloudflare

1. Open [Chaturbate](https://chaturbate.com) in your browser and complete the Cloudflare check.

    (Keep refresh with F5 if the check doesn't appear)

2. Preferred quick-refresh path: in Firefox DevTools, use **Network** → find a successful `chatvideocontext` or `biocontext` request → **Copy as cURL** → paste it into the Web UI `Browser Import` box.

3. Manual path: **DevTools (F12)** → **Application** → **Cookies** → `https://chaturbate.com`

    For the most reliable results, copy the full cookie string used by Firefox for Chaturbate, not just `cf_clearance`.

![sshot-2025-04-30-146](https://github.com/user-attachments/assets/69f4061b-29a2-48a7-ad57-0c86148805e2)

4. User-Agent can be found using [WhatIsMyBrowser](https://www.whatismybrowser.com/detect/what-is-my-user-agent/), now run with `-cookies` and `-user-agent`:

    ```bash
    $ ./goondvr -u yamiodymel \
        -cookies "cf_clearance=...; csrftoken=...; __cf_bm=...; agreeterms=1" \
        -user-agent "PASTE_YOUR_USER_AGENT_HERE"
    ```

    Example:

    ```bash
    $ ./goondvr -u yamiodymel \
        -cookies "cf_clearance=i975JyJSMZUuEj2kIqfaClPB2dLomx3.iYo6RO1IIRg-1746019135-1.2.1.1-2CX...; csrftoken=...; __cf_bm=..." \
        -user-agent "Mozilla/5.0 (Windows NT 10.0; Win64; x64)..."
    ```

    You can also paste a full header directly:

    ```bash
    $ ./goondvr -u yamiodymel \
        -cookies "Cookie: cf_clearance=...; csrftoken=...; __cf_bm=...; agreeterms=1" \
        -user-agent "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:154.0) Gecko/20100101 Firefox/154.0"
    ```

## 🌐 Browser Fallback

If Cloudflare starts blocking copied cookies and headers, the app can fall back to a real Chromium session for Chaturbate API requests.

### Local Mode

Use this when the DVR and browser profile live on the same machine:

```bash
$ ./goondvr --browser-mode local --browser-path chromium --browser-profile-dir ./conf/browser-profile
```

After you have manually passed the challenge once with that Chromium profile, the app will reuse that persistent browser session for later Chaturbate API fetches.

### Remote Helper Mode

Use this when the DVR runs on a headless server and a separate VM has the browser session.

Run the helper on the VM:

```bash
$ ./goondvr --browser-helper \
    --browser-path chromium \
    --browser-profile-dir ./conf/browser-profile \
    --browser-helper-bind 0.0.0.0:8091 \
    --browser-bootstrap \
    --browser-bootstrap-url https://chaturbate.com/ \
    --browser-helper-token "CHANGE_ME"
```

Then point the DVR server at it:

```bash
$ ./goondvr --browser-mode remote \
    --browser-helper-url http://VM_IP:8091 \
    --browser-helper-token "CHANGE_ME"
```

The Web UI exposes the same `Browser Fallback` settings, plus a `Browser Import` textarea for quick cookie/User-Agent refreshes.

When `--browser-bootstrap` is enabled in helper mode, the helper opens a visible Chromium window automatically using the same profile directory it later reuses for background fetches. This is the easiest way to solve the Cloudflare challenge on a desktop VM without running a separate manual Chromium command first.

If a channel is offline, the browser fallback now classifies it as offline rather than surfacing a Cloudflare block just because the direct API request was challenged first.

## 🕵️ Record Private Shows

1. Login [Chaturbate](https://chaturbate.com) in your browser.

2. **DevTools (F12)** → **Application** → **Cookies** → `https://chaturbate.com` → Copy the `sessionid` value

3. Run with `-cookies`:

    ```bash
    $ ./goondvr -u yamiodymel -cookies "sessionid=PASTE_YOUR_SESSIONID_HERE"
    ```

# 📄 Filename Pattern

The format is based on [Go Template Syntax](https://pkg.go.dev/text/template), available variables are:

`{{.Username}}`, `{{.Site}}`, `{{.Year}}`, `{{.Month}}`, `{{.Day}}`, `{{.Hour}}`, `{{.Minute}}`, `{{.Second}}`, `{{.Sequence}}`

By default, it hides the sequence if it's zero.

```
Pattern: {{if ne .Site "chaturbate"}}{{.Site}}/{{end}}{{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}{{if .Sequence}}_{{.Sequence}}{{end}}
 Output: yamiodymel_2024-01-02_13-45-00.ts    # Sequence won't be shown if it's zero.
 Output: stripchat/yamiodymel_2024-01-02_13-45-00_1.ts
```

**👀 or... The sequence can be shown even if it's zero.**

```
Pattern: {{.Username}}_{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}_{{.Sequence}}
 Output: yamiodymel_2024-01-02_13-45-00_0.ts
 Output: yamiodymel_2024-01-02_13-45-00_1.ts
```

**📁 or... Folder per each channel.**

```
Pattern: video/{{.Username}}/{{.Year}}-{{.Month}}-{{.Day}}_{{.Hour}}-{{.Minute}}-{{.Second}}_{{.Sequence}}
 Output: video/yamiodymel/2024-01-02_13-45-00_0.ts
```

_Note: Legacy HLS streams are saved as `.ts`. LL-HLS streams are saved as `.mp4` with muxed video and audio._

# 🛟 Closing the program, crashes and backups

- **Closing the console window** stops every recording within a few seconds (Windows ends the process about 5 s after the window is closed). Slow work such as the ffmpeg conversion is not started then; the recordings are listed in `conf/open-recordings.json` and finished the next time the program starts (the unfinished end is cut off and the seek index is built).
- **Recordings that were cut short** (power loss, task killed, full disk) are repaired the same way at the next start. Only the incomplete end of a file is removed.
- **`conf/channels.json` and `conf/settings.json`** are written atomically (a copy is written and swapped in), so a crash cannot leave a half-written file. A backup of each is kept in `conf/backup/` (at start, then every 10 minutes while they change, and when the program closes; the last 30 are kept). If a file is damaged at start, the newest good backup is restored and the damaged file is kept next to it as `*.corrupt-<time>`.

# 🤔 Frequently Asked Questions

**Q: The program closes immediately on Windows.**

> Open it via **Command Prompt**, the error message should appear. If needed, [create an issue](https://github.com/HeapOfChaos/goondvr/issues).

&nbsp;

**Q: Error `listen tcp :8080: bind: An attempt was... by its access permissions`**

> The port `8080` is in use. Try another port with `-p 8123`, then visit [http://localhost:8123](http://localhost:8123).
>
> If that fails, run **Command Prompt** as Administrator and execute:
>
> ```bash
> $ net stop winnat
> $ net start winnat
> ```

**Q: Error `A connection attempt failed... host has failed to respond`**

> Likely a network issue (e.g., VPN, firewall, or blocked by Chaturbate). This cannot be fixed by the program.

**Q: Error `Channel was blocked by Cloudflare`**

> You've been temporarily blocked. See the [Cookies & User-Agent](#-cookies--user-agent) section to bypass.

&nbsp;

**Q: Is Proxy or SOCKS5 supported?**

> Yes. You can launch the program using the `HTTPS_PROXY` environment variable:
>
> ```bash
> $ HTTPS_PROXY="socks5://127.0.0.1:9050" ./goondvr -u CHANNEL_USERNAME
> ```
