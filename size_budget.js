'use strict';
// size_budget.js — plan an H.264 encode that lands UNDER a byte ceiling.
//
// Why this exists: the publisher (igrunner/media_pre.py `download_media`)
// fetches the finished render through the cloud Bot API `getFile`, which
// refuses any file over 20 MiB. A CRF-paced render at the shipped settings
// produced 21,760,015 B for a 44 s source — 788 KB over the cap — so the exact
// final artifact became UNDOWNLOADABLE and publishing was structurally
// impossible. Size is therefore a hard constraint, not a quality preference.
//
// The owner's rule is explicit: meet the limit WITHOUT silently degrading
// quality or dropping effects, and if the limit cannot be met at an acceptable
// standard, FAIL LOUDLY and report the constraint. So this module has a floor
// (MIN_VIDEO_BPS): below it, planEncode() refuses rather than returning a
// bitrate that would quietly turn the Reel to mush.
//
// Pure, dependency-free, and the single source of truth: both the worker and
// its test require this file, so the number the test proves is the number the
// renderer uses.

// The cloud Bot API's getFile ceiling, with a margin. Telegram's own limit is
// 20 MiB (20,971,520 B); we budget below it so the muxer's real output — never
// exactly the arithmetic guess — still fits. 18 MiB leaves ~14% headroom.
const BOT_API_CAP = 20 * 1024 * 1024;
const SAFE_MARGIN = 0.90;
const DEFAULT_MAX_BYTES = Math.floor(BOT_API_CAP * SAFE_MARGIN); // ~18.9 MiB

// A floor below which 1080x1920 H.264 stops being acceptable for a Reel. Meta
// recommends ~3.5 Mbps for 1080p upload; 1.2 Mbps is the lowest this project is
// willing to call "the same product". Under it we refuse and report.
const DEFAULT_MIN_VIDEO_BPS = 1_200_000;
const DEFAULT_AUDIO_BPS = 128_000;
// Container/frame overhead on top of the elementary streams. Measured at ~4%
// on the shipped muxer settings; 1.06 is a deliberate over-estimate so the
// first pass usually lands under the ceiling.
const MUX_OVERHEAD = 1.06;

/**
 * Bytes → a video bitrate that keeps (video + audio + mux) under `maxBytes`.
 *
 * @param {object} o
 * @param {number} o.durationSec   real source duration (> 0)
 * @param {number} [o.maxBytes]    hard ceiling for the finished file
 * @param {number} [o.audioBps]    audio bitrate the graph will use
 * @param {number} [o.minVideoBps] refusal floor
 * @returns {{ok:true, videoBps:number, audioBps:number, maxBytes:number,
 *            totalBps:number, reason:string}
 *          |{ok:false, reason:string, neededBps:number, floorBps:number}}
 */
function planEncode(o) {
  const durationSec = Number(o && o.durationSec);
  const maxBytes = Number((o && o.maxBytes) || DEFAULT_MAX_BYTES);
  const audioBps = Number((o && o.audioBps) || DEFAULT_AUDIO_BPS);
  const minVideoBps = Number((o && o.minVideoBps) || DEFAULT_MIN_VIDEO_BPS);

  if (!Number.isFinite(durationSec) || durationSec <= 0)
    return { ok: false, reason: 'duration unknown — cannot budget bytes', neededBps: 0, floorBps: minVideoBps };
  if (!Number.isFinite(maxBytes) || maxBytes <= 0)
    return { ok: false, reason: 'maxBytes must be > 0', neededBps: 0, floorBps: minVideoBps };

  // Total bits we may spend per second, then subtract audio and overhead.
  const totalBps = Math.floor((maxBytes * 8) / durationSec);
  const videoBps = Math.floor(totalBps / MUX_OVERHEAD - audioBps);

  if (videoBps < minVideoBps) {
    return {
      ok: false,
      reason: `a ${durationSec.toFixed(1)}s Reel cannot fit ${maxBytes} B at an acceptable quality `
        + `(needs ${videoBps} bps of video, floor is ${minVideoBps} bps)`,
      neededBps: videoBps,
      floorBps: minVideoBps,
    };
  }
  return {
    ok: true,
    videoBps, audioBps, maxBytes, totalBps,
    reason: `budget ${videoBps} bps video + ${audioBps} bps audio over ${durationSec.toFixed(1)}s `
      + `→ ≤ ${maxBytes} B`,
  };
}

/**
 * One corrective pass: if a render overshot despite the plan, scale the
 * bitrate by how far it overshot. Deterministic, bounded to a single retry by
 * the caller — the alternative (CRF + guess) cannot guarantee a byte ceiling.
 */
function correctVideoBps(plannedVideoBps, actualBytes, maxBytes, audioBps = DEFAULT_AUDIO_BPS) {
  const ratio = Number(maxBytes) / Math.max(1, Number(actualBytes));
  // Shrink by the overshoot, but never below the caller's floor (the caller
  // re-checks the floor after correcting).
  const next = Math.floor(Number(plannedVideoBps) * ratio);
  return next > 0 ? Math.max(1, next - Math.floor(audioBps / 2)) : Math.floor(plannedVideoBps / 2);
}

module.exports = {
  planEncode, correctVideoBps,
  BOT_API_CAP, DEFAULT_MAX_BYTES, DEFAULT_MIN_VIDEO_BPS, DEFAULT_AUDIO_BPS, MUX_OVERHEAD,
};
