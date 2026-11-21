# igwatch-render-worker

Public deployment source for the IGWatchBot FFmpeg render worker. FadeHost
clones this repo to build the app; the worker then receives its own scoped
configuration at provisioning time.

**This repository contains no secrets.** All configuration arrives as
environment variables set by the IGWatchBot server through the FadeHost
management API. Nothing here holds a bot token, a management token, an account
id, or customer data.

## What it does

Claims a render job from the bot, downloads the source, renders a vertical
1080×1920 H.264/AAC MP4 (black frame + rounded corners + kept audio) under a
size budget, and returns the result. Media moves through the bot's scoped
gateway (`/api/worker_media`), so the host never needs the full bot token.

## Environment (set by IGWatchBot, not by hand)

| Variable | Purpose |
|---|---|
| `WORKER_ID` | this host's id (minted by IGWatchBot) |
| `WORKER_TOKEN` | this host's own credential (scoped, revocable) |
| `TG_BASE` | the bot's API base (enables the scoped media gateway) |

Legacy hosts may instead carry `FFM_TOKEN`, `BOT_TOKEN`, `ARCHIVE_CHAT` — kept
for backward compatibility with the installed host; the zero-touch path does
not need them.

## Run

```
node ffmpeg_worker.js
```

Requires Node 20+ and a local `ffmpeg` on `PATH` (or set `FFMPEG`). The mask
PNG is generated on first start; caches live under `/data` when present.