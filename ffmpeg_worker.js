// ffmpeg_worker.js — FadeHost native-FFmpeg worker (replaces the Cloudinary
// render path). Runs on the FadeHost FREE plan (256MB / 0.25 vCPU / 3GB disk).
//
// One process, one job at a time (concurrency=1). No second bot poller, no
// Telegram update handler — only outbound HTTPS calls to the bot's
// /api/worker_ffm endpoint (claim / heartbeat / complete / fail).
//
// The output is the EXACT spec (see lib/ffmqueue.js + the r92_local_ffmpeg
// sample this was ported from):
//   canvas 1080×1920, black bg, inner video 864×1536 centered (80%),
//   rounded corners r=32 via a real alpha mask, H.264 + AAC, audio kept.
//
// Memory strategy for the 256MB ceiling:
//   - single FFmpeg process, threads=1, preset ultrafast
//   - the alpha mask is a single 864×1536 PNG read once and reused per render
//     (generated on first start; not per-job)
//   - the source is downloaded to /tmp, rendered, then DELETED after a
//     successful submit (spec §4: free temp files after archive registration)
//   - if the source exceeds the size/duration caps, the job is failed with a
//     clear error — NO OOM loop, NO unbounded retry
//
// Required env (set in FadeHost, NEVER in source or logs):
//   TG_BASE        https://<bot>-api.tgcloud.app
//   FFM_TOKEN      value of the bot's worker_ffm_token setting (legacy, global)
//   WORKER_ID      optional registry id — set when the host was added under
//                  «تنظیمات → سرورهای پردازش». Enables per-host capacity and
//                  drain mode. Unset = the default single worker, unchanged.
//   WORKER_TOKEN   per-host bearer issued by the registry (takes precedence
//                  over FFM_TOKEN). Revoking this host never affects others.
//   ARCHIVE_CHAT  private archive channel id (where results are uploaded)
//   BOT_TOKEN     the Telegram bot token, used ONLY here, in the secure env
//   HEARTBEAT_S   heartbeat interval, default 30
//   FFMPEG        optional path to the ffmpeg binary (default: 'ffmpeg')
//
// The mask PNG (rounded-rect alpha for 864×1536, r=32) is written by the
// buildMask() below on first start if it is missing. It is a static asset,
// not per-job state.

const FS = require('fs');
const PATH = require('path');
const ZLIB = require('zlib');
const OS = require('os');
const CP = require('child_process');
const CRYPTO = require('crypto');
// Publisher-visible size cap: the igrunner fetches the finished render through
// the cloud Bot API `getFile`, which refuses anything over 20 MiB. A CRF-paced
// render overshot that once (21,760,015 B) and the artifact became
// undownloadable — so the bitrate is now budgeted from the real duration.
const { planEncode, correctVideoBps, DEFAULT_MIN_VIDEO_BPS: MIN_VIDEO_BPS } = require('./size_budget.js');

// --- TG_APP_ID / ingress tokens --------------------------------------------
// TG_APP_ID  Telegram Serverless app id, used to build the manage API base
//            (cloud.telegram.org/app<id>/manage/run) when TG_MGR_BASE is unset.
// TG_MGR_TOKEN  the project credential (app<id>:secret) from .tgcloud/credentials —
//            the ONLY thing needed to run deployed modules via the manage API,
//            which bypasses the Mini App init-data gate on tgcloud.ai.
const TG_APP_ID = process.env.TG_APP_ID || '';
const MGR_BASE = (process.env.TG_MGR_BASE || `https://cloud.telegram.org/app${TG_APP_ID}`).replace(/\/+$/, '');
const MGR_TOKEN = process.env.TG_MGR_TOKEN || '';
const FHM_TOKEN  = process.env.FHM_TOKEN  || '';
const OWNER_CHAT = Number(process.env.OWNER_CHAT || 0);
const ARCHIVE_CHAT = Number(process.env.ARCHIVE_CHAT || 0);
const BOT_TOKEN = process.env.BOT_TOKEN || '';
const HEARTBEAT_S = Number(process.env.HEARTBEAT_S || 30);

// safeText: read response body as text. Never throws; callers always get ''
// on any error including an already-locked stream.
async function safeText(res, label) {
  if (!res.body) return '';
  let reader;
  try {
    reader = res.body.getReader();
  } catch (e) {
    // Body already locked — another reader consumed it.
    console.error(`[safeText] body locked (${label || '?'}) — stream already consumed`);
    return '';
  }
  try {
    const chunks = [];
    try {
      for (;;) {
        const { done, value } = await reader.read();
        if (done) break;
        chunks.push(value);
      }
    } catch (e) {
      try { reader.cancel(); } catch {}
      return Buffer.concat(chunks).toString('utf8').slice(0, 512);
    }
    try { reader.releaseLock(); } catch {}
    return Buffer.concat(chunks).toString('utf8');
  } catch (e) {
    try { reader.cancel(); } catch {}
    try { reader.releaseLock(); } catch {}
    return '';
  }
}

// Gate 16 canvas constants. OUT_W/OUT_H are the ONLY shipped output size;
// the inner window is derived from the recipe (`innerWpct`) and the corner
// radius from `cornerRadius`, so both are defaults rather than hard rules.
const PW = 864, PH = 1536, R = 32; // default inner video size + corner radius
const OUT_W = 1080, OUT_H = 1920;
const MASK_PNG = PATH.join(__dirname, 'inner_mask_864x1536_r32.png');

// --- ffmpeg binary resolution (bootstrap, no 168MB in git) ------------------
// The BtbN linux64 GPL build is ~150MB tar.xz; we do NOT ship it in the repo.
// On first boot the worker bootstrap-downloads the pinned build into
// FFMPEG_CACHE_DIR, extracts only bin/ffmpeg, verifies it runs, and reuses
// the cached binary on every later boot.
//
// FadeHost wipes the app dir on every redeploy (~6 min free-tier cadence),
// so the cache MUST live on the persistent /data volume (which survives
// redeploy). Default to /data when present; only fall back to the app-local
// dir when /data is absent (e.g. local dev run).
//
// The download is RESUME-able and NON-BLOCKING so the worker never crash-
// loops while it is in flight:
//   - streams to a .part file and resumes from its current size on restart
//     (Range request), so a crash/redeploy mid-download does not restart
//     from 0;
//   - the queue loop never awaits it: ffmpeg is checked lazily before each
//     claim, so the worker stays alive (claim/heartbeat/recovery) even while
//     a download is in flight; a job is only claimed once ffmpeg is ready.
const FFMPEG_BUILD = 'ffmpeg-master-latest-linux64-gpl';
// Pinned artifact (~150MB, x86-64, links system glibc>=4.18); verified by
// running -version before first use.
const FFMPEG_URL = `https://github.com/BtbN/FFmpeg-Builds/releases/download/latest/${FFMPEG_BUILD}.tar.xz`;
const FFMPEG_CACHE_DIR = process.env.FFMPEG_CACHE_DIR || (FS.existsSync('/data') ? PATH.join('/data', 'ffmpeg_cache') : PATH.join(__dirname, 'ffmpeg_cache')); // /data survives FadeHost redeploys; app-local fallback for dev only
const FFMPEG_BIN = PATH.join(FFMPEG_CACHE_DIR, 'bin', 'ffmpeg');
const FFMPEG_PART = PATH.join(FFMPEG_CACHE_DIR, `${FFMPEG_BUILD}.tar.xz.part`);
const FFMPEG_READY_FLAG = PATH.join(FFMPEG_CACHE_DIR, '.ffmpeg-ready');

function ffmpegCached() {
  if (!FS.existsSync(FFMPEG_BIN)) return false;
  try { CP.execFileSync(FFMPEG_BIN, ['-version'], { stdio: 'ignore' }); return true; }
  catch { return false; }
}
function ffmpegReadyFlag() { try { return FS.existsSync(FFMPEG_READY_FLAG); } catch { return false; } }

// One background, resume-aware download. Idempotent: resumes from the .part
// size, extracts+verifies on success, writes the ready flag, deletes the .part.
// Extraction is tarball-format-agnostic: unpack into a scratch dir under the
// cache root, then move the found binary into <cache>/bin/ffmpeg regardless
// of the top-level dir name the build ships with (BtbN uses the build name,
// so this works for any *.tar.xz pinned below).
let bootstrapPromise = null;
// Stream one response body into .part, resume-aware; then extract, verify,
// write the ready flag, clean up scratch+.part.
async function downloadAndExtract(res, scratch, resumed) {
  const fd = FS.openSync(FFMPEG_PART, resumed ? 'r+' : 'w');
  let offset = resumed ? FS.statSync(FFMPEG_PART).size : 0;
  try {
    let reader;
    try { reader = res.body.getReader(); }
    catch (e) {
      // Body locked — prior interrupted download left stream consumed.
      // Delete .part so next retry fetches cleanly from 0.
      FS.closeSync(fd); FS.unlinkSync(FFMPEG_PART);
      throw new Error('ffmpeg download body locked (interrupted prior fetch) — deleted .part, retry fresh');
    }
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      FS.writeSync(fd, value, 0, value.length, offset);
      offset += value.length;
      if (offset % (5 * 1024 * 1024) < value.length) console.log(`[ffmpeg] downloaded ${Math.round(offset / 1048576)} MB`);
    }
    try { reader.releaseLock(); } catch {}
  } finally { try { FS.closeSync(fd); } catch {} }
  // Extract the whole archive into the scratch dir (no GNU --wilds needed),
  // then move the first bin/ffmpeg found up to <cache>/bin/ffmpeg and
  // delete scratch. This is portable across BtbN build names.
  const tarCmd = `tar -xJf '${FFMPEG_PART}' -C '${scratch}' 2>/dev/null || unxz -c '${FFMPEG_PART}' | tar -xf - -C '${scratch}'; src=$(find '${scratch}' -type f -name ffmpeg -path '*/bin/*' | head -1); if [ -z "$src" ]; then echo 'ffmpeg binary not found in archive' >&2; exit 1; fi; mkdir -p '${PATH.join(FFMPEG_CACHE_DIR, 'bin')}'; cp -f "$src" '${FFMPEG_BIN}'; chmod +x '${FFMPEG_BIN}'; rm -rf '${scratch}'; rm -f '${FFMPEG_PART}'`;
  CP.execSync(tarCmd, { stdio: 'pipe', timeout: 5 * 60 * 1000 });
  if (!ffmpegCached()) throw new Error('ffmpeg bootstrap: binary not extracted or not runnable');
  FS.writeFileSync(FFMPEG_READY_FLAG, String(Date.now()));
  console.log(`[ffmpeg] cached + verified at ${FFMPEG_BIN}`);
  return true;
}

function startFfmpegBootstrap() {
  if (bootstrapPromise) return bootstrapPromise;
  bootstrapPromise = (async () => {
    try {
      if (process.env.FFMPEG && process.env.FFMPEG !== 'ffmpeg') return true;
      if (ffmpegReadyFlag() || ffmpegCached()) return true;
      FS.mkdirSync(FFMPEG_CACHE_DIR, { recursive: true });
      const scratch = PATH.join(FFMPEG_CACHE_DIR, '_extract');
      FS.rmSync(scratch, { recursive: true, force: true });
      FS.mkdirSync(scratch, { recursive: true });
      // Resume: continue from the existing .part size, 0 on first boot.
      const startBytes = FS.existsSync(FFMPEG_PART) ? FS.statSync(FFMPEG_PART).size : 0;
      const headers = {};
      if (startBytes > 0) headers['Range'] = `bytes=${startBytes}-`;
      console.log(`[ffmpeg] ${startBytes > 0 ? `resuming download at ${Math.round(startBytes / 1048576)} MB` : 'downloading'} pinned build into ${FFMPEG_CACHE_DIR}`);
      let res = await fetch(FFMPEG_URL, { headers, signal: AbortSignal.timeout(10 * 60 * 1000) });
      // 416 = Range start >= server file size: a previous run already downloaded
      // the whole file but crashed before extraction deleted the .part. Drop
      // it and fetch once more from the top; the download is cheap relative
      // to the extraction+verify step that actually keeps state.
      if (res.status === 416) {
        console.log('[ffmpeg] .part already holds the full file (416) — re-downloading to extract');
        FS.rmSync(FFMPEG_PART, { force: true });
        res = await fetch(FFMPEG_URL, { signal: AbortSignal.timeout(10 * 60 * 1000) });
        if (res.status !== 200) throw new Error(`ffmpeg download HTTP ${res.status}`);
        try { return await downloadAndExtract(res, scratch, false); }
        catch (e) {
          // If body locked from a prior interrupted download, clean it up before
          // the outer catch re-throws. A locked body on 416 means the previous
          // .part fetch left the stream consumed — we must release it so the
          // next worker restart can retry cleanly.
          if (res.body) { try { await res.body.cancel(); } catch {} }
          throw e;
        }
      }
      if (res.status !== 200 && res.status !== 206) throw new Error(`ffmpeg download HTTP ${res.status}`);
      return await downloadAndExtract(res, scratch, res.status === 206);
    } catch (e) {
      console.error(`[ffmpeg] bootstrap failed (will retry on next restart): ${e.message}`);
      return false;
    } finally {
      bootstrapPromise = null;
    }
  })();
  return bootstrapPromise;
}

// True when an ffmpeg binary is usable right now; else kick off (or await)
// the resume-aware bootstrap once and re-check. Cheap to call per claim.
async function ensureFfmpeg() {
  if (process.env.FFMPEG && process.env.FFMPEG !== 'ffmpeg') return true;
  if (ffmpegCached() || ffmpegReadyFlag()) return true;
  await startFfmpegBootstrap();
  return ffmpegCached();
}
// Binary path for runFfmpeg(): $FFMPEG if set, else the cached one, else PATH.
function ffmpegPath() {
  if (process.env.FFMPEG && process.env.FFMPEG !== 'ffmpeg') return process.env.FFMPEG;
  if (ffmpegCached()) return FFMPEG_BIN;
  return 'ffmpeg';
}

// --- idle backoff + gateway 429 handling ------------------------------------
// The manage-API request budget is SHARED app-wide (forensic audit 2026-11-01):
// this worker's idle loop and any tgcloud CLI call draw from the same pool.
// So idle polling backs off exponentially (30→60→120→300s), and a gateway 429
// is honored with the server-requested wait (bounded, jittered) instead of an
// immediate re-poll. Idle polling sends NO heartbeat requests — heartbeats run
// only for an actively claimed job (startHeartbeat, in processJob). Orphan/
// crash-recovery sweeps are available on demand via fhm_control 'sweep'.
const IDLE_BACKOFF_S = [30, 60, 120, 300];
const MAX_429_WAIT_S = 300;
const startIdleDelay = () => IDLE_BACKOFF_S[0];
function nextIdleDelay(curS) {
  const i = IDLE_BACKOFF_S.indexOf(curS);
  if (i === -1 || i >= IDLE_BACKOFF_S.length - 1) return IDLE_BACKOFF_S[IDLE_BACKOFF_S.length - 1];
  return IDLE_BACKOFF_S[i + 1];
}
// Retry-After: response header first, then the gateway's "retry after N" text.
function parseRetryAfter(res, bodyText) {
  const h = res && res.headers && typeof res.headers.get === 'function'
    ? res.headers.get('retry-after') : null;
  if (h) { const n = Number(h); if (Number.isFinite(n) && n >= 0) return n; }
  const m = /retry after (\d+)/i.exec(String(bodyText || ''));
  return m ? Number(m[1]) : null;
}
// Build the error the main loop waits on. Unparseable 429 → the MAX backoff.
// Bounded ±20% jitter so concurrent callers never re-sync on the same tick.
function makeRateLimitError(res, bodyText) {
  let s = parseRetryAfter(res, bodyText);
  if (s == null) s = MAX_429_WAIT_S;
  s = Math.min(s, MAX_429_WAIT_S);
  const j = s * 0.2;
  const ms = Math.round((s - j + Math.random() * 2 * j) * 1000);
  const e = new Error(`HTTP 429 rate limited (wait ${Math.round(ms / 1000)}s)`);
  e.rateLimited = true;
  e.waitMs = ms;
  return e;
}

// --- worker_ffm endpoint ----------------------------------------------------
// The tgcloud.ai public ingress only accepts verified Mini App init data, so
// an external POST gets 401 before the endpoint runs. The manage API
// (cloud.telegram.org/app<id>/manage/run) is externally reachable with the
// project credential token; call it as a fallback when TG_BASE is 401.
let TG_BASE = process.env.TG_BASE || '';
// FFM_TOKEN is the legacy single-worker bearer (`settings.worker_ffm_token`).
// WORKER_TOKEN + WORKER_ID are the registry route (P4): a host registered in
// «تنظیمات → سرورهای پردازش» gets its own credential, so revoking one host
// cannot break the others. Unset WORKER_ID keeps the exact old behaviour.
const FFM_TOKEN = process.env.FFM_TOKEN || '';
const WORKER_ID = String(process.env.WORKER_ID || '').replace(/[^a-zA-Z0-9_-]/g, '').slice(0, 40);
const WORKER_TOKEN = process.env.WORKER_TOKEN || '';
const BEARER = WORKER_TOKEN || FFM_TOKEN;
// Every control-plane call needs a deadline. Node's fetch has none by default,
// so one stalled connection (a dead TG_BASE ingress, a half-open socket) parked
// the whole claim loop forever: no error, no claim stamp, no log line — the
// worker looked alive (the igrunner pipe kept logging) while the render queue
// sat untouched. A timeout turns that into an ordinary [loop] error + backoff.
const FF_HTTP_TIMEOUT_MS = 60_000;

// POST one worker_ffm action. Tries TG_BASE first; on 401 falls back to the
// manage API (which bypasses the Mini App init-data gate).
async function ffm(action, body) {
  const payload = JSON.stringify({ token: BEARER, workerId: WORKER_ID, action, ...body });
  const headers = { 'content-type': 'application/json' };
  if (TG_BASE) {
    try {
      const res = await fetch(`${TG_BASE}/api/worker_ffm`, { method: 'POST', headers, body: payload, signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS) });
      if (res.status === 429) {
        const raw429 = await safeText(res, `ffm.${action}.429`);
        throw makeRateLimitError(res, raw429);
      }
      if (res.status === 401 && MGR_TOKEN) return await ffmViaManage(action, body);
      const raw = await safeText(res, `ffm.${action}.primary`);
      if (!res.ok) {
        if (res.status === 401 && MGR_TOKEN) return await ffmViaManage(action, body);
        throw new Error(`worker_ffm ${action} HTTP ${res.status}: ${raw.slice(0, 200)}`);
      }
      let out; try { out = JSON.parse(raw); } catch { throw new Error(`worker_ffm ${action} HTTP ${res.status}: ${raw.slice(0, 200)}`); }
      return out;
    } catch (e) {
      if (e && e.rateLimited) throw e; // honor the 429 wait; do NOT fall back on it
      if (MGR_TOKEN && /401|fetch|ECONN|ETIMEDOUT/.test(String(e.message || e))) return await ffmViaManage(action, body);
      throw e;
    }
  }
  if (MGR_TOKEN) return await ffmViaManage(action, body);
  if (TG_BASE) {
    // re-throw the original TG_BASE error path if no manage fallback configured
    const res = await fetch(`${TG_BASE}/api/worker_ffm`, { method: 'POST', headers, body: payload, signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS) });
    const raw = await safeText(res, `ffm.${action}.fallback`);
    if (!res.ok) throw new Error(`worker_ffm ${action} HTTP ${res.status}: ${raw.slice(0, 200)}`);
    let out; try { out = JSON.parse(raw); } catch { throw new Error(`worker_ffm ${action} HTTP ${res.status}: ${raw.slice(0, 200)}`); }
    return out;
  }
  throw new Error('no TG_BASE or TG_MGR_TOKEN configured');
}

// Execute the deployed endpoints/worker_ffm module via the manage /run API.
// `sources` must include the module + every transitive import (lib/*, schema);
// an empty `sources:{}` makes the platform look up a cached module that is
// not stored there — MODULE_NOT_FOUND. The sources are fetched from the
// manage /get endpoint on EVERY boot; the local JSON file is only a fallback
// for when that fetch itself fails (see loadMgrSources).
const MGR_SOURCES_FILE = PATH.join(__dirname, '.mgr_sources.json');
let mgrSources = null;
async function loadMgrSources(refresh) {
  if (mgrSources && !refresh) return mgrSources;
  // The canonical sources are fetched fresh on every boot. They used to be
  // read from a disk cache FIRST, and that cache is untracked so it survives
  // the deploy's `git reset` — the worker then executed whatever bot-side
  // module version happened to be cached on the day it was written, silently
  // (claim polls succeeding against an OLD worker_ffm, e.g. one without the
  // liveness stamp, is how that shows up). A stale bot module is worse than a
  // failed boot, so disk is only a fallback for when the fetch itself fails.
  let res = null;
  try {
    res = await fetch(`${MGR_BASE}/manage/get`, {
      headers: { 'authorization': `Bearer ${MGR_TOKEN}` },
      signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS),
    });
  } catch (e) {
    // Fetch unavailable: fall back to the last known-good snapshot, if any.
    try {
      const cached = JSON.parse(FS.readFileSync(MGR_SOURCES_FILE, 'utf8'));
      if (cached && cached['endpoints/worker_ffm']) {
        console.log(`[mgr] manage/get failed (${String(e.message || e).slice(0, 80)}); using cached sources`);
        mgrSources = cached;
        return mgrSources;
      }
    } catch {}
    throw e;
  }
  const raw = await safeText(res, 'loadMgrSources.manageGet');
  let env; try { env = JSON.parse(raw); } catch { env = {}; }
  const mods = env.result?.canonical_modules || {};
  // worker_ffm's transitive imports: lib/settings (KEY), lib/ffmqueue,
  // schema; plus the fhm_control endpoint (enqueue/status/pause/resume/list).
  const picked = {};
  for (const k of Object.keys(mods)) {
    if (k === 'endpoints/worker_ffm' || k === 'endpoints/fhm_control' ||
        k === 'lib/settings' || k === 'lib/ffmqueue' || k === 'schema')
      picked[k] = mods[k];
  }
  // An empty/partial answer must never be used as `sources`: it makes the
  // platform resolve a cached module (MODULE_NOT_FOUND) or, worse, a stale one.
  if (!picked['endpoints/worker_ffm']) {
    try {
      const cached = JSON.parse(FS.readFileSync(MGR_SOURCES_FILE, 'utf8'));
      if (cached && cached['endpoints/worker_ffm']) {
        console.log('[mgr] manage/get returned no worker_ffm; using cached sources');
        mgrSources = cached;
        return mgrSources;
      }
    } catch {}
    throw new Error('manage/get returned no canonical worker_ffm module');
  }
  mgrSources = picked;
  try { FS.writeFileSync(MGR_SOURCES_FILE, JSON.stringify(mgrSources)); } catch {}
  return mgrSources;
}

async function ffmViaManage(action, body) {
  if (!MGR_TOKEN || !MGR_BASE) throw new Error('manage fallback requires TG_MGR_BASE + TG_MGR_TOKEN');
  const sources = await loadMgrSources();
  const res = await fetch(`${MGR_BASE}/manage/run`, {
    method: 'POST',
    headers: {
      'content-type': 'application/json',
      'authorization': `Bearer ${MGR_TOKEN}`,
    },
    signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS),
    body: JSON.stringify({
      module: 'endpoints/worker_ffm',
      sources,
      args: { token: BEARER, workerId: WORKER_ID, action, ...body },
      ctx: {},
    }),
  });
  const raw = await safeText(res, `ffmViaManage.${action}`);
  if (res.status === 429) throw makeRateLimitError(res, raw); // honor gateway 429; no immediate re-poll
  let env; try { env = JSON.parse(raw); } catch { env = {}; }
  if (!res.ok || !env.ok) {
    const desc = String(env.description || `manage/run HTTP ${res.status}`);
    // A stale cached source (a bot-side module was redeployed under us) shows
    // up as a module error, a dropped action, or an old-endpoint shape error
    // (e.g. the pre-change worker_ffm 'jobId required'). Refetch the canonical
    // sources once and retry before giving up.
    if (/MODULE|unknown action|not found|jobId required/i.test(desc)) {
      const sources2 = await loadMgrSources(true);
      const res2 = await fetch(`${MGR_BASE}/manage/run`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${MGR_TOKEN}` },
        signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS),
        body: JSON.stringify({ module: 'endpoints/worker_ffm', sources: sources2, args: { token: BEARER, workerId: WORKER_ID, action, ...body }, ctx: {} }),
      });
      const t2 = await safeText(res2, `ffmViaManage.${action}.retry`);
      let env2;
      try { env2 = JSON.parse(t2); } catch { env2 = {}; }
      if (res2.ok && env2.ok) {
        mgrSources = sources2; // keep the fresh cache for subsequent calls
        const r2 = env2.result?.result;
        if (!r2 || r2.ok === false) throw new Error(`worker_ffm ${action} via manage: ${JSON.stringify(env2.result).slice(0, 200)}`);
        return r2;
      }
    }
    throw new Error(`worker_ffm ${action} via manage: ${desc.slice(0, 200)}`);
  }
  // /run wraps the module's return in { result, time, log }; the endpoint's
  // own return is at .result.result.
  const r = env.result?.result;
  if (!r || r.ok === false) {
    // Heartbeat shape changed under us (owner/state fields missing) → the
    // cached sources are stale; refetch once and retry.
    if (action === 'heartbeat' && !(r.owner || r.state)) {
      const sources2 = await loadMgrSources(true);
      const res2 = await fetch(`${MGR_BASE}/manage/run`, {
        method: 'POST',
        headers: { 'content-type': 'application/json', authorization: `Bearer ${MGR_TOKEN}` },
        signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS),
        body: JSON.stringify({ module: 'endpoints/worker_ffm', sources: sources2, args: { token: BEARER, workerId: WORKER_ID, action, ...body }, ctx: {} }),
      });
      const t2 = await safeText(res2, `ffmViaManage.${action}.heartbeatRetry`);
      let env2;
      try { env2 = JSON.parse(t2); } catch { env2 = {}; }
      const r2 = env2.result?.result;
      if (env2.ok && r2 && (r2.owner || r2.state)) {
        mgrSources = sources2;
        return r2;
      }
    }
    throw new Error(`worker_ffm ${action} via manage: ${JSON.stringify(env.result).slice(0, 200)}`);
  }
  return r;
}

// fhm() — the Control API caller: enqueue / status / pause / resume / list.
// Uses the manage API transport (the only reliable external ingress) with
// the independent FHM_TOKEN. Distinct from ffm() (worker_ffm endpoint:
// claim/heartbeat/complete/fail).
async function fhm(action, body) {
  if (!FHM_TOKEN || !MGR_TOKEN || !MGR_BASE)
    throw new Error('FHM_TOKEN + TG_MGR_TOKEN + TG_APP_ID required for Control API');
  const sources = await loadMgrSources();
  const res = await fetch(`${MGR_BASE}/manage/run`, {
    method: 'POST',
    headers: { 'content-type': 'application/json', 'authorization': `Bearer ${MGR_TOKEN}` },
    signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS),
    body: JSON.stringify({
      module: 'endpoints/fhm_control',
      sources,
      args: { token: FHM_TOKEN, action, ...body },
      ctx: {},
    }),
  });
  const raw = await safeText(res, `fhm.${action}`);
  if (res.status === 429) throw makeRateLimitError(res, raw); // honor gateway 429
  let env; try { env = JSON.parse(raw); } catch { env = {}; }
  if (!res.ok || !env.ok) {
    throw new Error(`fhm ${action}: ${env.description || `HTTP ${res.status}`}`);
  }
  const r = env.result?.result;
  if (!r || r.ok === false) {
    throw new Error(`fhm ${action}: ${JSON.stringify(r || env.result).slice(0, 200)}`);
  }
  return r;
}

// --- the rounded-rect alpha mask, generated per geometry --------------------
// Gate 16: the inner window and corner radius come from the owner's saved
// recipe, so the mask is built from (w, h, r) rather than fixed constants.
// `dest` defaults to the legacy static path so a dev run still has one.
function buildMask(w = PW, h = PH, r = R, dest = MASK_PNG) {
  const raw = Buffer.alloc(w * h * 4);
  const r2 = r * r;
  for (let y = 0; y < h; y++) {
    for (let x = 0; x < w; x++) {
      const inside =
        (x >= r && x < w - r) || (y >= r && y < h - r) ||
        ((x < r && y < r && (x - r) * (x - r) + (y - r) * (y - r) <= r2) ||
         (x >= w - r && y < r && (x - (w - r)) * (x - (w - r)) + (y - r) * (y - r) <= r2) ||
         (x < r && y >= h - r && (x - r) * (x - r) + (y - (h - r)) * (y - (h - r)) <= r2) ||
         (x >= w - r && y >= h - r && (x - (w - r)) * (x - (w - r)) + (y - (h - r)) * (y - (h - r)) <= r2));
      const a = inside ? 255 : 0;
      const o = (y * w + x) * 4;
      raw[o] = 255; raw[o + 1] = 255; raw[o + 2] = 255; raw[o + 3] = a;
    }
  }
  const crcTable = [];
  for (let n = 0; n < 256; n++) {
    let c = n;
    for (let k = 0; k < 8; k++) c = (c & 1) ? (0xEDB88320 ^ (c >>> 1)) : (c >>> 1);
    crcTable[n] = c >>> 0;
  }
  const crc32b = (b) => {
    let c = 0xFFFFFFFF;
    for (let i = 0; i < b.length; i++) c = crcTable[(c ^ b[i]) & 0xFF] ^ (c >>> 8);
    return (c ^ 0xFFFFFFFF) >>> 0;
  };
  const chunk = (type, data) => {
    const len = Buffer.alloc(4); len.writeUInt32BE(data.length);
    const td = Buffer.concat([Buffer.from(type), data]);
    const crc = Buffer.alloc(4); crc.writeUInt32BE(crc32b(td));
    return Buffer.concat([len, td, crc]);
  };
  const sig = Buffer.from([137, 80, 78, 71, 13, 10, 26, 10]);
  const ihdr = Buffer.alloc(13);
  ihdr.writeUInt32BE(w, 0); ihdr.writeUInt32BE(h, 4);
  ihdr[8] = 8; ihdr[9] = 6; // 8-bit RGBA
  const withFilter = Buffer.alloc(h * (1 + w * 4));
  for (let y = 0; y < h; y++) {
    withFilter[y * (1 + w * 4)] = 0;
    raw.copy(withFilter, y * (1 + w * 4) + 1, y * w * 4, (y + 1) * w * 4);
  }
  FS.writeFileSync(dest, Buffer.concat([
    sig, chunk('IHDR', ihdr), chunk('IDAT', ZLIB.deflateSync(withFilter)), chunk('IEND', Buffer.alloc(0)),
  ]));
  console.log(`[mask] wrote ${dest} (${FS.statSync(dest).size} B)`);
}

// Recipe → geometry. The owner's saved profile drives the inner window; the
// defaults reproduce the original 864×1536/r32 output exactly. An aspect-
// preserving fit happens inside the window (no stretch, rule 7).
function geometryFor(recipe) {
  const cfg = recipe && typeof recipe === 'object' ? recipe : {};
  const pct = Number(cfg.innerWpct);
  const wpct = Number.isFinite(pct) && pct > 0 && pct <= 100 ? pct : 80;
  const innerW = Math.round((OUT_W * wpct) / 100 / 2) * 2;
  // 9:16 inner window: the canvas keeps the same ratio, so a vertical source
  // fills it exactly and a non-vertical one is letterboxed, never cropped.
  const innerH = Math.round((innerW * OUT_H) / OUT_W / 2) * 2;
  const rad = Number(cfg.cornerRadius);
  const r = Number.isFinite(rad) && rad >= 0 ? Math.min(Math.round(rad), Math.floor(innerH / 2)) : R;
  return { innerW, innerH, r, x: Math.round((OUT_W - innerW) / 2), y: Math.round((OUT_H - innerH) / 2),
           bg: bgColorFor(cfg) };
}

// Rule 6: the background colour the owner picked must reach the file. Only a
// plain #rrggbb is accepted — anything else (or a missing value) is the black
// default, so a malformed profile can never produce a broken filter graph.
function bgColorFor(cfg) {
  const c = String(cfg.bgColor || '').trim();
  return /^#[0-9a-fA-F]{6}$/.test(c) ? c : '#000000';
}

// --- source download via Bot API (archive file → /tmp) -----------------------
// Bot API 5xx (incl. 504 Gateway Timeout) and 429 are transient: bounded
// retry with jitter to prevent thundering-herd bursts. Only truly safe ops
// are auto-retried:
//   - getFile (read-only: safe to repeat)
//   - sendVideo with a duplicate guard (re-checked before each retry)
// A per-call AbortSignal timeout bounds each attempt so a hung connection
// cannot wedge the whole job.
const BOT_API_TIMEOUT_MS = 120_000; // 120s — bounds sendVideo upload on slow connections; archive sends can be large (20MB+)
const RETRY_DELAYS_MS = [2_000, 8_000, 20_000]; // ≤3 retries, then give up
// Jitter: ±20% random spread breaks up simultaneous retries from multiple workers
const jitter = (ms) => ms + Math.floor(Math.random() * ms * 0.4) - Math.floor(ms * 0.2);
function isTransientBotError(out, status) {
  // 4xx = client error (bad token, bad chat, bad file) — NOT retryable.
  // 429/5xx = transient (server throttled / gateway 504) — retryable.
  if (status === 429) return true;
  return status >= 500;
}

// Scoped media gateway (endpoints/worker_media.js). When BOT_TOKEN is absent
// (the zero-touch path: the host only holds WORKER_ID + WORKER_TOKEN), the
// worker moves media through the bot instead of talking to Telegram directly,
// so it never needs the full bot token. Same bearer as worker_ffm.
const USE_GATEWAY = !BOT_TOKEN && !!TG_BASE;
async function gwCall(action, body) {
  if (!USE_GATEWAY) throw new Error('gateway not configured');
  const payload = JSON.stringify({ token: BEARER, workerId: WORKER_ID, action, ...body });
  const res = await fetch(`${TG_BASE}/api/worker_media`, {
    method: 'POST', headers: { 'content-type': 'application/json' }, body: payload,
    signal: AbortSignal.timeout(FF_HTTP_TIMEOUT_MS),
  });
  const raw = await safeText(res, `gw.${action}`);
  if (!res.ok) throw new Error(`worker_media ${action} HTTP ${res.status}: ${raw.slice(0, 160)}`);
  let out; try { out = JSON.parse(raw); } catch { throw new Error(`worker_media ${action}: bad JSON`); }
  return out;
}

async function botApi(method, params) {
  let lastErr = 'unknown botApi error';
  for (let attempt = 0; attempt <= RETRY_DELAYS_MS.length; attempt++) {
    let out, status;
    try {
      const res = await fetch(`https://api.telegram.org/bot${BOT_TOKEN}/${method}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify(params),
        signal: AbortSignal.timeout(BOT_API_TIMEOUT_MS),
      });
      status = res.status;
      // Use text() + JSON.parse instead of res.json(): text() always consumes
      // the body stream and releases the lock, so a retry sees a fresh stream.
      // res.json() can lock the stream on a non-JSON error (e.g. HTML gateway
      // page on 429) — subsequent retries would fail "ReadableStream locked".
      const raw = await safeText(res, `botApi.${method}`);
      try { out = JSON.parse(raw); } catch { out = { ok: false, description: `HTTP ${status}: ${raw.slice(0, 200)}` }; }
    } catch (e) {
      // If safeText itself threw (stream was locked), fall back to raw='' rather
      // than calling safeText(res) again on the same — now-locked — response.
      lastErr = `botApi ${method}: ${e.name === 'TimeoutError' ? 'request timed out (60s)' : e.message}`;
      // Network-level errors are transient — retryable.
      if (attempt < RETRY_DELAYS_MS.length) {
        await sleep(jitter(RETRY_DELAYS_MS[attempt]));
        continue;
      }
      throw new Error(lastErr);
    }
    if (out.ok) return out.result;
    lastErr = `botApi ${method}: ${out.description || status}`;
    if (!isTransientBotError(out, status) || attempt >= RETRY_DELAYS_MS.length)
      throw new Error(lastErr); // 4xx or last attempt — stop, no retry
    // Transient (429/5xx) — respect a server-provided retry_after if present.
    const ra = out.parameters?.retry_after;
    const baseDelay = (typeof ra === 'number' ? Math.ceil(ra * 1000) : RETRY_DELAYS_MS[attempt]);
    const delay = jitter(baseDelay);
    console.warn(`[botApi] ${method} transient (${status}) — retry ${attempt + 1}/${RETRY_DELAYS_MS.length} in ${Math.round(delay / 1000)}s`);
    await sleep(delay);
  }
  throw new Error(lastErr);
}

// Pre-send duplicate guard for a video upload: scan the channel's recent
// history for a prior video with the SAME jobId marker in its caption.
// `marker` is the unique per-job string embedded in our caption. Returns the
// existing message { message_id, file_id } if already sent, else null.
async function findExistingSend(chatId, marker, limit = 20) {
  try {
    const hist = await botApi('getChatHistory', { chat_id: chatId, limit });
    const msgs = Array.isArray(hist) ? hist : (hist?.messages || []);
    for (const m of msgs) {
      const cap = m?.video?.caption || m?.caption || '';
      if ((m?.video || m?.media_group) && String(cap).includes(marker)) {
        return { message_id: m.message_id, file_id: m.video?.file_id || null };
      }
    }
  } catch (e) {
    // History lookup is a best-effort guard: if it fails, fall back to the
    // pre-send check by NOT skipping — we send once and rely on the jobId
    // marker; a second send for the same job is still guarded by a later
    // check. Failing closed on history lookup is safer than silently
    // skipping a send that hasn't happened.
    console.warn(`[dup-guard] getChatHistory ${chatId} failed (${e.message}) — proceeding with send`);
  }
  return null;
}

// Download the job's source THROUGH the bot (no BOT_TOKEN on this host). The
// gateway scopes by job ownership and serves base64 chunks; we poll until the
// reported total is collected, then write atomically.
async function downloadViaGateway(fileId, dest, jobId) {
  const part = dest + '.part';
  const fd = FS.openSync(part, 'w');
  let offset = 0, total = null;
  try {
    for (let guard = 0; guard < 5000; guard++) {
      const c = await gwCall('source', { jobId, start: offset, len: 1500000 });
      if (!c || !c.ok) throw new Error('gateway source: ' + (c && c.error || 'failed'));
      total = c.total;
      if (c.data) { const buf = Buffer.from(c.data, 'base64'); FS.writeSync(fd, buf); offset += buf.length; }
      if (offset >= total) break;
      if (!c.data) throw new Error('gateway source: empty chunk before total');
    }
  } finally { FS.closeSync(fd); }
  if (total != null && offset !== total) throw new Error(`gateway source short read ${offset}/${total}`);
  FS.renameSync(part, dest);
  return offset;
}

async function downloadSource(fileId, dest, jobId) {
  if (USE_GATEWAY) return downloadViaGateway(fileId, dest, jobId);
  const f = await botApi('getFile', { file_id: fileId });
  const url = `https://api.telegram.org/file/bot${BOT_TOKEN}/${f.file_path}`;
  // Resume-capable download: a crash/hang mid-download leaves a .part file;
  // a retry resumes from its size (Range) instead of starting over. The dest
  // is written atomically (rename) so a render never sees a truncated source.
  const part = dest + '.part';
  const startBytes = FS.existsSync(part) ? FS.statSync(part).size : 0;
  const headers = startBytes > 0 ? { Range: `bytes=${startBytes}-` } : {};

  let res;
  try {
    res = await fetch(url, { headers, signal: AbortSignal.timeout(5 * 60_000) });
  } catch (e) {
    // Network-level error (hang / timeout / ECONN) — clean the .part and retry.
    FS.unlinkSync(part);
    res = await fetch(url, { headers: {}, signal: AbortSignal.timeout(5 * 60_000) });
    if (!res.ok && res.status !== 206) throw new Error(`source download HTTP ${res.status}`);
    // Fresh stream — fall through to download
  }

  if (!res.ok && res.status !== 206) {
    // Non-2xx: drain the body so the stream is released before retry.
    // Node.js keeps the body readable until fully consumed; we drain it to
    // unlock the stream. Then retry from 0 (previous .part may be garbage).
    try { await res.body.cancel(); } catch {}
    FS.unlinkSync(part);
    res = await fetch(url, { signal: AbortSignal.timeout(5 * 60_000) });
    if (!res.ok) throw new Error(`source download HTTP ${res.status}`);
  }

  const fd = FS.openSync(part, startBytes > 0 ? 'r+' : 'w');
  let offset = startBytes;
  try {
    let reader;
    try { reader = res.body.getReader(); }
    catch (e) {
      // Body locked — retry from scratch via botApi wrapper.
      try { FS.closeSync(fd); } catch {}
      FS.unlinkSync(part);
      throw new Error('source download body locked — retrying from 0');
    }
    for (;;) {
      const { done, value } = await reader.read();
      if (done) break;
      FS.writeSync(fd, value, 0, value.length, offset);
      offset += value.length;
    }
    try { reader.releaseLock(); } catch {}
  } catch (e) {
    try { reader.cancel(); } catch {}
    try { FS.closeSync(fd); } catch {}
    FS.unlinkSync(part);
    throw e; // botApi retry wrapper above will handle it
  } finally { try { FS.closeSync(fd); } catch {} }
  FS.renameSync(part, dest);
  return FS.statSync(dest).size;
}

// --- upload the rendered MP4 to the private archive channel ------------------
// Upload the rendered MP4 to BOTH the private archive channel AND the owner's
// private chat. Each chat gets a unique per-job marker (FFMJOB:<jobId>) in the
// caption, so a re-claim after a crash can detect a prior successful send and
// REUSE its message instead of posting a duplicate. Returns { outMsgId,
// outFileId }.
// Upload the finished render THROUGH the bot. The gateway assembles the chunks
// and archives with the bot token server-side, then records outMsgId/outFileId
// on the job. We return the same shape uploadToChats does.
async function uploadViaGateway(job, localPath) {
  const bytes = FS.readFileSync(localPath);
  const CHUNK = 1500000;
  for (let off = 0; off < bytes.length; off += CHUNK) {
    const slice = bytes.subarray(off, Math.min(off + CHUNK, bytes.length));
    const done = off + slice.length >= bytes.length;
    const r = await gwCall('result', {
      jobId: job.jobId, start: off, data: slice.toString('base64'),
      done, totalBytes: bytes.length,
    });
    if (!r || !r.ok) throw new Error('gateway upload: ' + (r && r.error || 'failed'));
    if (done) return { outMsgId: r.msgId ?? null, outFileId: null, bytes: r.bytes ?? bytes.length };
  }
  return { outMsgId: null, outFileId: null, bytes: bytes.length };
}

async function uploadToChats(job, localPath, filename) {
  if (USE_GATEWAY) return uploadViaGateway(job, localPath);
  const bytes = FS.readFileSync(localPath);
  const marker = 'FFMJOB:' + job.jobId;
  const cap = (extra) => '🎬 ' + filename + ' — ' + marker + ' ' + extra;
  const sendVideo = (chatId, captionText) => {
    const form = new FormData();
    form.append('chat_id', String(chatId));
    form.append('caption', captionText);
    form.append('video', new Blob([bytes], { type: 'video/mp4' }), filename);
    // Explicit dimensions + streaming flag: Telegram shows the correct
    // 9:16 player geometry before the client fetches full metadata.
    form.append('width', String(OUT_W));
    form.append('height', String(OUT_H));
    form.append('supports_streaming', 'true');
    return fetch('https://api.telegram.org/bot' + BOT_TOKEN + '/sendVideo', {
      method: 'POST', body: form,
      signal: AbortSignal.timeout(BOT_API_TIMEOUT_MS),
    });
  };
  // 1) Archive channel (source of truth, registered msg + file_id).
  // Duplicate guard: if a prior run already posted this job, reuse its message.
  let prior = await findExistingSend(ARCHIVE_CHAT, marker);
  if (!prior) {
    // Mark job as 'uploading' BEFORE sending. Then send with AbortError handling:
    // if timeout, the video MAY have arrived at Telegram (Telegram completes before
    // responding). Mark ambiguous so the next worker checks findExistingSend and
    // finds the already-delivered message → completes safely without duplicate.
    try {
      await ffm('uploading', { jobId: job.jobId });
      const res1 = await sendVideo(ARCHIVE_CHAT, cap('خروجی موتور FFmpeg (قاب مشکی + گوشه گرد)'));
      const raw1 = await safeText(res1, 'uploadToChats.archive');
      let out1; try { out1 = JSON.parse(raw1); } catch { out1 = { ok: false, description: 'HTTP ' + res1.status }; }
      if (!out1.ok) {
        for (const d of RETRY_DELAYS_MS) {
          await sleep(d);
          prior = await findExistingSend(ARCHIVE_CHAT, marker);
          if (prior) break;
          let r2;
          try { r2 = await sendVideo(ARCHIVE_CHAT, cap('خروجی موتور FFmpeg (قاب مشکی + گوشه گرد)')); }
          catch (err2) {
            if (err2.name === 'AbortError') {
              await ffm('fail', { jobId: job.jobId, error: 'upload timeout — ambiguous (may have been sent)', ambiguous: true });
              return { outMsgId: null, outFileId: null };
            }
            throw err2;
          }
          const t2 = await safeText(r2, 'uploadToChats.archiveRetry');
          let o2; try { o2 = JSON.parse(t2); } catch { o2 = { ok: false, description: 'HTTP ' + r2.status }; }
          if (o2.ok) { prior = { message_id: o2.result.message_id, file_id: o2.result.video?.file_id || o2.result.file_id }; break; }
          console.warn('[upload] archive send retry failed: ' + o2.description);
        }
        if (!prior) throw new Error('sendVideo archive: ' + (out1.description || 'transient 5xx after retries'));
      } else {
        prior = { message_id: out1.result.message_id, file_id: out1.result.video?.file_id || out1.result.file_id };
      }
    } catch (err) {
      if (err.name === 'AbortError') {
        await ffm('fail', { jobId: job.jobId, error: 'upload timeout — ambiguous (may have been sent)', ambiguous: true });
        return { outMsgId: null, outFileId: null };
      }
      throw err;
    }
  }
  const outMsgId = prior.message_id;
  const outFileId = prior.file_id;

  // 2) The owner's copy is the bot's job, no longer the worker's. The bot
  // replies to `complete` with the publish-ready card (final video + buttons,
  // worker_ffm.sendPublishCard), so a second bare send here would duplicate
  // the exact bytes. Removing it also stops a broken/TG-capped OWNER_CHAT from
  // failing or throttling the upload path — the archive is the source of truth
  // either way. (This worker still returns outMsgId/outFileId from the archive.)
  return { outMsgId, outFileId };
}

// --- the actual render (single FFmpeg process, threads=1, ultrafast) ---------
// Geometry comes from the owner's recipe (rules 5/6); the fit inside the inner
// window PRESERVES the source aspect ratio (rule 7: no stretch, no crop):
// `force_original_aspect_ratio=decrease` scales the source so it fits wholly
// inside the window, then `pad` centers it on the (already rounded) black
// window. A horizontal source is therefore letterboxed, not stretched.
function buildFilterGraph({ innerW: IW, innerH: IH, x, y, bg = '#000000' }) {
  // The mask PNG is RGBA (alpha holds the rounded-rect shape). `format=gray`
  // DROPS the alpha channel and reads all-white luma → NO corner rounding at
  // all (pixel-verified 2026-11-03: bottom corners fully square). The alpha
  // plane IS the mask — extract it directly.
  // `bg` is the owner's pick from the edit profile (rule 6) — it fills both the
  // canvas outside the window and the letterbox bars inside it, so the frame
  // reads as one colour.
  // PERF: fast_bilinear scaling + removed setsar (measured 2026-11-12: 6% faster
  // on FadeHost 0.25 vCPU, same visual output for 9:16 content).
  // `r=30` is NOT optional: `color` defaults to 25 fps and it is the overlay's
  // main input, so dropping it silently re-timed every render to 25 fps
  // (ffprobe-verified 2026-11-13). Keep it.
  const color = String(bg).replace('#', '0x');
  return [
    `[0:v]scale=${IW}:${IH}:force_original_aspect_ratio=decrease:flags=fast_bilinear,` +
      `pad=${IW}:${IH}:(ow-iw)/2:(oh-ih)/2:color=${color}[vs]`,
    `[1:v]format=yuva420p,extractplanes=a[mask]`,
    `[vs][mask]alphamerge[va]`,
    `color=c=${color}:s=${OUT_W}x${OUT_H}:r=30[bg]`,
    `[bg][va]overlay=${x}:${y},format=yuv420p[o]`,
  ].join(';');
}

function runFfmpegAsync(src, mask, out, geo, budget) {
  const t0 = Date.now();
  const filter = buildFilterGraph(geo);
  const args = [
    '-v', 'error', '-y',
    '-threads', '1',
    '-i', src, '-i', mask,
    '-filter_complex', filter,
    '-map', '[o]', '-map', '0:a', '-map', '0:s?',
    '-c:v', 'libx264', '-preset', 'ultrafast',
    // Bitrate-budgeted, not CRF: the finished file MUST land under the Bot API
    // getFile cap or the publisher cannot fetch the exact artifact at all. CRF
    // cannot promise a byte ceiling. `-maxrate`/`-bufsize` keep a hard scene
    // from spiking past the average. Effects, geometry and audio are untouched.
    '-b:v', String(budget.videoBps),
    '-maxrate', String(budget.videoBps),
    '-bufsize', String(budget.videoBps * 2),
    '-pix_fmt', 'yuv420p',
    '-c:a', 'aac', '-b:a', `${Math.round(budget.audioBps / 1000)}k`,
    '-c:s', 'mov_text',
    '-movflags', '+faststart',
    '-shortest', out,
  ];
  return new Promise((resolve, reject) => {
    const child = CP.spawn(ffmpegPath(), args, { stdio: 'pipe' });
    let aborted = false;
    const watcher = setInterval(() => {
      if (currentJob?._leaseLost && !aborted) {
        aborted = true;
        child.kill('SIGKILL');
      }
    }, 1000);
    const drain = (stream) => { let s = ''; stream.on('data', (d) => { s += d; }); return () => s; };
    const getErr = drain(child.stderr);
    child.on('error', (e) => { clearInterval(watcher); reject(e); });
    child.on('close', (code) => {
      clearInterval(watcher);
      if (aborted) { reject(new Error('render aborted: lease lost mid-render')); return; }
      if (code !== 0) { reject(new Error(`ffmpeg exit ${code}: ${getErr().slice(0, 300)}`)); return; }
      resolve(Date.now() - t0);
    });
  });
}

// Real peak-RAM probe: the cgroup v2 peak if available, else the process RSS.
function peakRamBytes() {
  try {
    const p = FS.readFileSync('/sys/fs/cgroup/memory.peak', 'utf8');
    return Number(p.trim() || 0);
  } catch {}
  try {
    const p = FS.readFileSync('/sys/fs/cgroup/memory/memory.usage_in_bytes', 'utf8');
    return Number(p.trim() || 0);
  } catch {}
  return process.memoryUsage().rss;
}

// --- one job ----------------------------------------------------------------
const MAX_SRC_BYTES = Number(process.env.MAX_SRC_BYTES || 20 * 1024 * 1024);
const MAX_SRC_SECS  = Number(process.env.MAX_SRC_SECS  || 120);
// One job at a time: the worker claims, downloads, renders, uploads, then
// completes. A re-claim by a newer instance (lease expiry) supersedes an
// in-flight job; the stale instance bails before its terminal write lands.
//
// LEASE-EXTENSION POLICY: a process only extends a lease it is actively
// rendering. Idle workers (between jobs) send heartbeats with no jobId — a
// liveness signal that keeps the connection warm without grabbing lease
// time. When a new instance claims a job, the stale instance stops rendering
// and stops extending that lease, so the new claimant owns the full lease
// window.
let currentJob = null;
function startHeartbeat() {
  if (currentJob._hb) return;
  currentJob._hb = setInterval(async () => {
    try {
      const r = await ffm('heartbeat', { jobId: currentJob.jobId });
      // Lost the lease (a later worker instance re-claimed the job): stop
      // extending someone else's lease and surface the takeover so the
      // process can bail out of the render cleanly.
      if (r.owner && r.owner !== 'fadehost_ffmpeg') {
        console.error(`[hb] lease lost for ${currentJob.jobId} (owner=${r.owner})`);
        currentJob._leaseLost = true;
      } else if (r.state && r.state !== 'processing' && r.state !== 'uploading') {
        console.error(`[hb] job ${currentJob.jobId} left processing (state=${r.state})`);
        currentJob._leaseLost = true;
      }
    } catch (e) {
      console.error('[hb]', e.message);
    }
  }, HEARTBEAT_S * 1000);
}

// --- source probe + Gate 16 vertical gate -----------------------------------
// The host ships ffmpeg but NOT ffprobe (the bootstrap extracts only that one
// binary), so dimensions/duration come from `ffmpeg -i` stderr — the same
// technique the Python publisher uses. Returns null when nothing can read it.
function probeSource(file) {
  try {
    const p = CP.spawnSync(ffmpegPath(), ['-hide_banner', '-i', file], { encoding: 'utf8', timeout: 20000 });
    const txt = String(p.stderr || '');
    let w = null, h = null, dur = null;
    const mv = /Stream #\d+:\d+.*?Video:.*?(\d{2,5})x(\d{2,5})/.exec(txt)
      || /Video:[^\n]*?(\d{2,5})x(\d{2,5})/.exec(txt);
    if (mv) { w = Number(mv[1]); h = Number(mv[2]); }
    const md = /Duration:\s*(\d+):(\d\d):(\d\d(?:\.\d+)?)/.exec(txt);
    if (md) dur = Number(md[1]) * 3600 + Number(md[2]) * 60 + Number(md[3]);
    const audio = /Stream #\d+:\d+.*?Audio:/.test(txt);
    return { w, h, dur, audio };
  } catch { return null; }
}

// Rule 2/3: the shipped product is vertical 9:16 only. A non-vertical source
// is REJECTED before it costs a render; conversion is allowed only when the
// owner explicitly opted in (`recipe.allowNonVertical`), which the menu sets
// behind its own confirm. A probe failure is also a refusal (fail-closed):
// without dimensions we cannot prove the input is vertical.
const VERTICAL_TOL = 0.06; // ±6% around 0.5625 lets 9:16-ish sources through
function assertVertical(probe, recipe) {
  if (recipe && recipe.allowNonVertical) return;
  if (!probe || !probe.w || !probe.h)
    throw new Error('ابعاد منبع تشخیص داده نشد — برای انتشار عمودی، ویدئو باید قابل خواندن و ۹:۱۶ باشد');
  const ratio = probe.w / probe.h;
  if (Math.abs(ratio - 9 / 16) > VERTICAL_TOL)
    throw new Error(`منبع ${probe.w}x${probe.h} (نسبت ${ratio.toFixed(3)}) عمودی ۹:۱۶ نیست — `
      + 'در حالت پیشفرض رد میشود؛ تبدیل فقط با انتخاب صریح مالک («تبدیل به عمودی») انجام میشود');
}

// Gate 16.3: the durable, first-hand record of the file that was just built.
// Everything here is measured from the real bytes on disk, before they are
// deleted — duration/fps/codecs are read back out of the muxed MP4 (NOT copied
// from the source), and the hash is the approval key the owner's preview can
// be bound to. Telegram's forward object omits duration and never carries a
// hash, so this is the only place the record can come from.
function sha256File(file) {
  const h = CRYPTO.createHash('sha256');
  h.update(FS.readFileSync(file));
  return h.digest('hex');
}

// fps + codec names via ffmpeg -i stderr (the host ships no ffprobe).
function describeStreams(file) {
  try {
    const p = CP.spawnSync(ffmpegPath(), ['-hide_banner', '-i', file], { encoding: 'utf8', timeout: 20000 });
    const txt = String(p.stderr || '');
    const fps = /(\d+(?:\.\d+)?)\s*fps/.exec(txt);
    const v = /Stream #\d+:\d+.*?Video:\s*([A-Za-z0-9_]+)/.exec(txt);
    const a = /Stream #\d+:\d+.*?Audio:\s*([A-Za-z0-9_]+)/.exec(txt);
    return { fps: fps ? Number(fps[1]) : null, codecV: v ? v[1] : null, codecA: a ? a[1] : null };
  } catch { return { fps: null, codecV: null, codecA: null }; }
}

function describeOutput(out, sizeBytes, srcProbe, outProbe, geo, recipe) {
  const st = describeStreams(out);
  const r = recipe && typeof recipe === 'object' ? recipe : {};
  return {
    // real, read back from the finished file
    width: outProbe.w, height: outProbe.h,
    durationSec: outProbe.dur,
    fps: st.fps,
    codecV: st.codecV, codecA: st.codecA,
    hasAudio: !!outProbe.audio,
    bytes: sizeBytes,
    sha256: sha256File(out),
    // the edit that produced it, so the hash is never orphaned from a profile
    profile: {
      name: r.name || null,
      innerWpct: r.innerWpct ?? 80,
      cornerRadius: geo.r,
      innerW: geo.innerW, innerH: geo.innerH,
      bg: geo.bg || '#000000',
      keepAudio: r.keepAudio !== false,
      logo: !!r.logoPublicId, text: !!r.textOn,
    },
    source: srcProbe ? { width: srcProbe.w, height: srcProbe.h, durationSec: srcProbe.dur } : null,
    renderedAt: Math.floor(Date.now() / 1000),
  };
}

async function processJob(job) {
  currentJob = job;
  const tmp = OS.tmpdir();
  const src = PATH.join(tmp, `${job.jobId}_src.mp4`);
  const out = PATH.join(tmp, `${job.jobId}_out.mp4`);
  startHeartbeat();
  try {
    // 1) source — hard caps checked BEFORE render (spec §4: overflow → clear error, no OOM loop)
    const bytes = await downloadSource(job.sourceFileId, src, job.jobId);
    if (bytes > MAX_SRC_BYTES) {
      FS.unlinkSync(src);
      throw new Error(`source ${bytes} B exceeds hard cap ${MAX_SRC_BYTES} B — stopping, no retry`);
    }
    if (job.sourceSeconds > MAX_SRC_SECS) {
      FS.unlinkSync(src);
      throw new Error(`source ${job.sourceSeconds}s exceeds cap ${MAX_SRC_SECS}s — stopping, no retry`);
    }
    // 2) Gate 16 vertical gate + recipe-driven geometry. The source must be
    // vertical 9:16 (or the owner explicitly allowed conversion), and the
    // inner window/corner radius come from the owner's saved profile so the
    // settings they see really reach the output file.
    const recipe = job.recipe || {};
    const probe = probeSource(src);
    assertVertical(probe, recipe);
    const geo = geometryFor(recipe);
    // Budget the encode from the SOURCE's real duration. A missing duration is
    // a refusal: without it there is no way to promise the byte ceiling, and
    // an over-cap artifact cannot be downloaded by the publisher at all.
    if (!probe || !probe.dur || probe.dur <= 0)
      throw new Error('مدت منبع خوانده نشد — بودجهٔ بایت قابل محاسبه نیست؛ رندر متوقف شد');
    const budget = planEncode({ durationSec: probe.dur });
    if (!budget.ok)
      throw new Error(`بودجهٔ حجم ممکن نیست: ${budget.reason} — کیفیت بیصدا پایین آورده نمیشود`);
    console.log(`[budget] ${job.jobId} ${budget.reason}`);
    const innerMask = PATH.join(tmp, `${job.jobId}_mask.png`);
    buildMask(geo.innerW, geo.innerH, geo.r, innerMask);
    // 3) render (rounded corners via the real alpha mask, aspect preserved).
    // The child process is spawned so we can abort it if the heartbeat flags a
    // lease loss: a superseded instance must not finish+upload a job a newer
    // instance already owns (two completes racing on one row).
    let renderMs = await runFfmpegAsync(src, innerMask, out, geo, budget);
    // Size is a hard post-condition, so an overshoot gets ONE corrective pass
    // at a lower bitrate rather than shipping an artifact nobody can download.
    let stat = FS.statSync(out);
    if (stat.size > budget.maxBytes) {
      const bps2 = correctVideoBps(budget.videoBps, stat.size, budget.maxBytes, budget.audioBps);
      // The corrected rate must still clear the quality floor: if meeting the
      // size cap would push the picture below an acceptable standard, we STOP
      // and report rather than shipping a degraded file (owner rule).
      if (bps2 < MIN_VIDEO_BPS || bps2 <= 0)
        throw new Error(`حجم خروجی ${stat.size} B از سقف ${budget.maxBytes} B گذشت و اصلاح آن کیفیت را `
          + 'زیر حد قابلقبول میبرد — رندر متوقف شد، هیچ فایلی آپلود نشد');
      console.log(`[budget] ${job.jobId} overshoot ${stat.size} B > ${budget.maxBytes} B → re-encoding at ${bps2} bps`);
      renderMs += await runFfmpegAsync(src, innerMask, out, geo,
        { videoBps: bps2, audioBps: budget.audioBps });
      stat = FS.statSync(out);
    }
    try { FS.unlinkSync(innerMask); } catch {}
    if (currentJob?._leaseLost) {
      // A later worker instance re-claimed this job while we rendered. Bail
      // out before the stale complete/fail lands on top of the new owner's
      // in-flight work.
      throw new Error('lease lost mid-render — superseded by a newer worker instance');
    }
    // 4) validate the REAL output (rule 9): dimensions must be 1080x1920 and
    // the audio presence must match the recipe. A render whose geometry or
    // audio does not match what the owner configured is a BLOCK, never a
    // silent pass — publishing an unverified file is the fallback rule 12 bans.
    stat = FS.statSync(out);
    if (stat.size < 5000) throw new Error(`output too small (${stat.size} B) — render likely failed`);
    const oprobe = probeSource(out);
    if (!oprobe || oprobe.w !== OUT_W || oprobe.h !== OUT_H)
      throw new Error(`خروجی نامعتبر: ${oprobe ? `${oprobe.w}x${oprobe.h}` : 'ابعاد خوانده نشد'} `
        + `— باید ${OUT_W}x${OUT_H} باشد؛ انتشار مسدود شد`);
    if (recipe.keepAudio !== false && probe && probe.audio && !oprobe.audio)
      throw new Error('خروجی صدا ندارد ولی پروفایل ویرایش «حفظ صدا» است — انتشار مسدود شد');
    // Print the real numbers the gate just checked. The artifact is too big for
    // the Bot API getFile cap (20 MB), so this log line is the only first-hand
    // metadata evidence available to the owner for a finished render.
    // Gate 16.3: capture the FULL record here, while the bytes still exist —
    // the temp file is deleted moments later and Telegram's forward object
    // reports duration=0, so nothing downstream can reconstruct this.
    const mediaMeta = describeOutput(out, stat.size, probe, oprobe, geo, recipe);
    console.log(`[verify] ${job.jobId} ${JSON.stringify(mediaMeta)}`);
    const peak = peakRamBytes();
    // 3) upload the result to the archive channel + owner's private chat.
    // The dup-guard marker is the jobId — a crash + re-claim never re-posts
    // a video the previous instance already delivered (mission 2026-10-30 §2).
    const { outMsgId, outFileId } = await uploadToChats(job, out, `ig_ffm_${job.label || job.jobId}.mp4`);
    // 4) free temp files AFTER successful archive registration (spec §4)
    FS.unlinkSync(src); FS.unlinkSync(out);
    await ffm('complete', {
      jobId: job.jobId, outMsgId, outFileId, renderMs,
      peakRamb: Math.round(peak / 1024), // KB
      meta: mediaMeta,
    });
    console.log(`[done] ${job.jobId} renderMs=${renderMs} outMsg=${outMsgId} outFile=${outFileId} peakKB=${Math.round(peak / 1024)}`);
  } catch (e) {
    // Distinguish a crashed-in-flight (ambiguous) from a hard failure.
    // A lease-lost bail is NOT ambiguous: the newer instance owns the job, so
    // report a plain failure (retriable, no retry-count burn beyond the
    // normal path) and let the new instance take over.
    const leaseLost = !!currentJob?._leaseLost;
    const ambiguous = !leaseLost && /heartbeat|connection|ECONN|ETIMEDOUT|fetch/i.test(String(e.message || e));
    try {
      FS.existsSync(src) && FS.unlinkSync(src);
      FS.existsSync(out) && FS.unlinkSync(out);
    } catch {}
    await ffm('fail', { jobId: job.jobId, error: String(e.message || e).slice(0, 300), ambiguous });
    console.error(`[fail] ${job.jobId} ${ambiguous ? 'ambiguous' : leaseLost ? 'lease-lost' : 'failed'}: ${e.message}`);
  } finally {
    if (currentJob?._hb) clearInterval(currentJob._hb);
    currentJob = null;
  }
}

// --- IG publish loop as a co-process ----------------------------------------
// Spawns igrunner.py and restarts it if it exits, with a 60s floor between
// attempts so a broken python env cannot spin. Any spawn/exit error is logged
// with the [igrunner] prefix and otherwise ignored: the render loop is the
// priority and must never be affected by the publisher's problems.
let igRunnerChild = null;
let igRunnerNext = 0;
function startIgRunner() {
  if (String(process.env.IG_RUNNER || '').toLowerCase() === 'off') {
    console.log('[igrunner] disabled by IG_RUNNER=off');
    return;
  }
  const tick = () => {
    if (Date.now() < igRunnerNext) return setTimeout(tick, 5000).unref();
    const script = require('path').join(__dirname, 'igrunner.py');
    let child;
    try {
      child = CP.spawn('python3', [script], {
        cwd: require('path').dirname(__dirname), stdio: ['ignore', 'pipe', 'pipe'],
        env: process.env,
      });
    } catch (e) {
      console.error('[igrunner] spawn failed:', e.message);
      igRunnerNext = Date.now() + 60_000;
      return setTimeout(tick, 5000).unref();
    }
    igRunnerChild = child;
    const pipe = (stream, sink) => stream && stream.on('data', (b) => {
      for (const line of String(b).split('\n')) if (line.trim()) sink(`[igrunner] ${line}`);
    });
    pipe(child.stdout, console.log);
    pipe(child.stderr, console.error);
    child.on('exit', (code, sig) => {
      igRunnerChild = null;
      console.error(`[igrunner] exited code=${code} sig=${sig || '-'} — retry in 60s`);
      igRunnerNext = Date.now() + 60_000;
      setTimeout(tick, 5000).unref();
    });
    child.on('error', (e) => console.error('[igrunner] error:', e.message));
  };
  tick();
}

// --- main loop: claim → process → wait a beat → repeat -----------------------
// Idle pacing: exponential backoff IDLE_BACKOFF_S (30→300s), reset on a
// successful claim. No idle heartbeats (see the backoff block above).
async function main() {
  // The bearer is either this host's own WORKER_TOKEN (the P4 registry route)
  // or the legacy global FFM_TOKEN. Requiring FFM_TOKEN by name would make a
  // host registered via «تنظیمات → سرورهای پردازش» — which sets only
  // WORKER_ID + WORKER_TOKEN — exit at boot even though it holds a valid
  // credential. Gate on the actual bearer.
  //
  // In GATEWAY mode (no BOT_TOKEN, TG_BASE set) the bot token and archive chat
  // live on the SERVER, not here — the zero-touch host needs neither.
  if (!BEARER) {
    console.error('WORKER_TOKEN (or FFM_TOKEN) required. Exiting.');
    process.exit(2);
  }
  if (!USE_GATEWAY && (!BOT_TOKEN || !ARCHIVE_CHAT)) {
    console.error('Legacy mode needs BOT_TOKEN + ARCHIVE_CHAT (or set TG_BASE only, for gateway mode). Exiting.');
    process.exit(2);
  }
  // The manage API is the live ingress (tgcloud.ai 401s external POSTs).
  // TG_BASE is kept only for the rare case where the platform later opens
  // external ingress. When neither TG_BASE nor TG_MGR_TOKEN is set, exit.
  if (!TG_BASE && !MGR_TOKEN) {
    console.error('Set TG_BASE or TG_MGR_TOKEN+TG_APP_ID to reach worker_ffm. Exiting.');
    process.exit(2);
  }
  if (!FS.existsSync(MASK_PNG)) buildMask();
  // Kick off the resume-aware ffmpeg bootstrap in the background (non-blocking).
  // The claim loop below gates on it: the worker stays alive during the ~150MB
  // first download (heartbeats / recovery keep working) and only claims a job
  // once an ffmpeg binary is actually runnable.
  startFfmpegBootstrap();
  // IG publish loop as a co-process. The FadeHost runtime starts only this
  // Node entrypoint, and `python3 fadehost_worker/igrunner.py` in a start
  // command would replace the render worker. The FFmpeg path above is
  // untouched; this is one detached sibling process with its own log prefix.
  // Disabled by IG_RUNNER=off; crashes here never touch the render loop.
  startIgRunner();
  console.log(`[start] worker up, mgr ${MGR_TOKEN ? MGR_BASE : '(no TG_MGR_TOKEN)'}, fhm ${FHM_TOKEN ? 'ok' : '(no FHM_TOKEN)'}, owner ${OWNER_CHAT || '(none)'}, ffmpeg bootstrap in progress`);
  // Backoff state lives OUTSIDE the loop: it was declared inside, so every
  // iteration reset it to 30s and the documented 30→300s ramp never happened —
  // an idle worker polled every 30s forever (2880 manage/run calls a day).
  let idleS = startIdleDelay();
  for (;;) {
    try {
      // Idle: don't block the loop on ffmpeg. No idle heartbeat — the shared
      // manage-API budget is reserved for real claims; liveness is implied by
      // the claim polls themselves (backing off 30→300s while idle/paused).
      if (!(await ensureFfmpeg())) {
        await sleep(idleS * 1000);
        idleS = nextIdleDelay(idleS);
        continue;
      }
      const r = await ffm('claim', {});
      console.log(`[loop] claim: paused=${r.paused} job=${r.job ? r.job.jobId : 'none'} idle=${idleS}s`);
      if (r.blocked) {
        // Registry said no: draining, disabled, or already at capacity. Back
        // off the same way an idle poll does — a blocked host must never spin.
        console.log(`[loop] claim blocked: ${r.blocked}${r.inflight != null ? ` (${r.inflight}/${r.capacity})` : ''}`);
        await sleep(idleS * 1000);
        idleS = nextIdleDelay(idleS);
        continue;
      }
      if (r.paused) {
        // Queue paused: same exponential backoff as idle. Re-polling still
        // detects a resume, just slower; backoff resets when unpaused work
        // actually resumes (a successful claim below).
        await sleep(idleS * 1000);
        idleS = nextIdleDelay(idleS);
        continue;
      }
      if (!r.job) {
        await sleep(idleS * 1000);
        idleS = nextIdleDelay(idleS);
        continue;
      }
      idleS = startIdleDelay(); // successful claim → normal cadence resumes
      await processJob(r.job);
    } catch (e) {
      console.error('[loop]', e.message);
      // A gateway 429 (from ffm/ffmViaManage/fhm) carries the server-requested,
      // jittered wait — honor it instead of the plain idle interval. Network
      // errors step the backoff too, so a dead endpoint never busy-loops.
      await sleep(e && e.rateLimited ? e.waitMs : idleS * 1000);
      idleS = e && e.rateLimited ? idleS : nextIdleDelay(idleS);
    }
  }
}
const sleep = (ms) => new Promise((res) => setTimeout(res, ms));
// Exported for the offline render-geometry test (test/ffmpeg_geometry.test.mjs).
// The host runs this file as the entrypoint, so the loop still starts there.
module.exports = { geometryFor, assertVertical, probeSource, buildMask, buildFilterGraph, OUT_W, OUT_H };
if (require.main === module) main();
