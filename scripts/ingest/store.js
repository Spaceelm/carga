/**
 * store.js
 *
 * Upstash Redis writer for normalized chargers.
 *
 * Keyspace (per design doc):
 *   chargers:<cc>:geo        GEO set, member = charger id      (GEOSEARCH BYBOX)
 *   charger:<cc>:<id>        JSON string, per-charger detail
 *   chargers:<cc>:ids        SET of live ids (bookkeeping for mark-and-sweep)
 *   chargers:<cc>:meta       JSON { lastRun, count, mode }
 *
 * Refresh safety (mark-and-sweep, never wipes live data on failure):
 *   1. Upsert every incoming charger (GEOADD + SET JSON) and collect its id.
 *   2. Only AFTER all upserts succeed, compute stale = previousIds - currentIds
 *      and remove those (ZREM + DEL). A crash/parse error before the sweep leaves
 *      the last-good dataset fully intact (extra-but-valid records at worst).
 *
 * A --dry-run mode skips all writes and just reports what would happen.
 *
 * Credentials come from env:  UPSTASH_REDIS_REST_URL / UPSTASH_REDIS_REST_TOKEN.
 */

const { Redis } = require('@upstash/redis');
const { CHARGER_STATUS } = require('./schema');

const GEO_KEY = (cc) => `chargers:${cc.toLowerCase()}:geo`;
const IDS_KEY = (cc) => `chargers:${cc.toLowerCase()}:ids`;
const META_KEY = (cc) => `chargers:${cc.toLowerCase()}:meta`;
const CHARGER_KEY = (cc, id) => `charger:${cc.toLowerCase()}:${id}`;
// Auxiliary indexes for the cheap status path (written at full refresh; stable
// between refreshes). pointindex: refill-point id -> charger id (PT status diff).
// coordindex: charger id -> [lat, lon] (ES marker-match sweep without MGET-all).
// lastfeed: previous status feed, compactly encoded against the pointindex order
// (see "Last-feed snapshot" below) — a delta cache, internal to the ingest and safe
// if lost: a missing or stale snapshot just costs one full-path run.
const POINTINDEX_KEY = (cc) => `chargers:${cc.toLowerCase()}:pointindex`;
const COORDINDEX_KEY = (cc) => `chargers:${cc.toLowerCase()}:coordindex`;
const LASTFEED_KEY = (cc) => `chargers:${cc.toLowerCase()}:lastfeed`;
// Resumable full-crawl checkpoint (providers rate-limited below one-run coverage,
// e.g. REVE at ~5 req/hr). crawl: JSON { nextPage, startedAt }; each fetched page is
// staged under crawl:page:<n> until the final page is reached, then a single
// upsert+sweep consumes them (see providers/reve.js crawlStatic).
const CRAWL_KEY = (cc) => `chargers:${cc.toLowerCase()}:crawl`;
const CRAWL_PAGE_KEY = (cc, n) => `chargers:${cc.toLowerCase()}:crawl:page:${n}`;

const PIPELINE_BATCH = 200; // records per pipeline flush

// ---- Chunked JSON values ----------------------------------------------------
// Upstash's REST API rejects requests over ~1 MB. PT's indexes fit comfortably,
// but FR's point index / last-feed snapshots (~165k charge points) serialize to
// several MB. Values at or under CHUNK_CHARS are stored in the plain key exactly
// as before (PT/ES unchanged, zero migration); larger ones are split into
// `<key>:c<i>` chunks with a `{"__chunks":N}` sentinel at the base key.
//
// Each chunk is stored JSON-encoded (a JSON string of the slice) so the Upstash
// client's automatic deserialization always yields the slice verbatim — a raw
// slice that happened to look like a JSON literal (e.g. all digits) would
// otherwise come back as a number. Slices never split a UTF-16 surrogate pair.
//
// Chunks are written BEFORE the sentinel: a crash mid-write leaves the previous
// sentinel pointing at a mixed set, which fails the reader's integrity checks and
// returns null — callers treat that as "index missing" and fall back to the
// always-correct full path. Shrunken writes may strand a few stale `:c` keys;
// they are unreadable garbage of bounded size, overwritten by the next growth.
// 400k chars, not ~1M: the cap applies to the REQUEST BODY, which is the slice
// escaped twice (chunkedSet JSON-encodes each slice, then the client serializes the
// command). Quote-heavy content can inflate a slice by ~2x, so a 700k threshold
// could still emit a >1 MB body. 400k keeps the worst case comfortably under the
// limit at the cost of a few extra SETs per full refresh.
const CHUNK_CHARS = Math.max(100, parseInt(process.env.INGEST_CHUNK_CHARS || '', 10) || 400000);

function safeParse(s) {
  try { return JSON.parse(s); } catch (_e) { return null; }
}

/**
 * Store `obj` as JSON under `key`, chunking transparently when it exceeds the
 * Upstash request cap. Returns the number of Redis commands used.
 */
async function chunkedSet(redis, key, obj) {
  const json = JSON.stringify(obj);
  if (json.length <= CHUNK_CHARS) {
    await redis.set(key, json);
    return 1;
  }
  const chunks = [];
  let i = 0;
  while (i < json.length) {
    let end = Math.min(i + CHUNK_CHARS, json.length);
    // Never split a surrogate pair across chunks.
    const c = end < json.length ? json.charCodeAt(end - 1) : 0;
    if (c >= 0xd800 && c <= 0xdbff) end -= 1;
    chunks.push(json.slice(i, end));
    i = end;
  }
  for (let k = 0; k < chunks.length; k++) {
    await redis.set(`${key}:c${k}`, JSON.stringify(chunks[k]));
  }
  await redis.set(key, JSON.stringify({ __chunks: chunks.length }));
  return chunks.length + 1;
}

/**
 * Read a value written by chunkedSet (or a legacy plain SET). Returns the parsed
 * object, or null when missing/corrupt — callers must treat null as "absent".
 */
async function chunkedGet(redis, key) {
  return chunkedGetFrom(redis, key, await redis.get(key));
}

/**
 * chunkedGet for a base value the caller already has. The status path fetches
 * meta + pointindex + lastfeed in ONE MGET, so the base bytes are in hand before
 * we know whether they are a plain value or a `{__chunks:N}` sentinel; this
 * resolves that without spending a second GET on the key we just read.
 */
async function chunkedGetFrom(redis, key, base) {
  if (base == null) return null;
  const parsed = typeof base === 'string' ? safeParse(base) : base;
  if (!parsed || typeof parsed !== 'object') return null;
  if (!Number.isInteger(parsed.__chunks)) return parsed; // plain (small/legacy) value
  const n = parsed.__chunks;
  if (n <= 0 || n > 10000) return null;
  const parts = await redis.mget(...Array.from({ length: n }, (_, k) => `${key}:c${k}`));
  const slices = [];
  for (const p of parts) {
    if (typeof p !== 'string' || p.length === 0) return null; // missing/mixed chunk
    // Chunks are JSON-encoded strings; the client may or may not have already
    // deserialized them. If it still looks like a JSON string, unwrap it.
    const un = p.charCodeAt(0) === 0x22 ? safeParse(p) : p;
    slices.push(typeof un === 'string' ? un : p);
  }
  return safeParse(slices.join(''));
}

// ---- Last-feed snapshot: compact, positional encoding -----------------------
// The delta path needs "what was each refill point's status last run?". The old
// shape was a plain `{pointId: status}` map — for FR that is ~60k ids x ~27 chars
// ≈ 1.6 MB, which chunkedSet splits into several chunks. Chunk WRITES cannot be
// batched into one MSET (each chunk is already sized against Upstash's ~1 MB
// request cap), so that map cost ~5 commands to write and 2 to read on EVERY run
// — the single largest fixed cost of a status run, and the thing that made a
// 10-minute cadence unaffordable.
//
// The ids are pure redundancy: the point index we already read enumerates exactly
// the same points. So store statuses POSITIONALLY against the point index's key
// order — one character per point. FR's ~165k points become a ~165k-char string:
// one plain SET, one plain GET, never chunked.
//
// Positional means the order must match. `g` is a fingerprint of the point-index
// key list; a full refresh rebuilds the index, the fingerprint changes, and the
// snapshot is rejected as unusable -> the run takes the always-correct full path
// and rewrites it. Losing a snapshot is never a correctness problem, only a cost
// one, so a conservative mismatch is exactly the right failure mode.
const LASTFEED_VERSION = 2;
const STATUS_TO_CODE = Object.freeze({
  [CHARGER_STATUS.AVAILABLE]: 'a',
  [CHARGER_STATUS.CHARGING]: 'c',
  [CHARGER_STATUS.OUT_OF_ORDER]: 'o',
  [CHARGER_STATUS.PLANNED]: 'p',
  [CHARGER_STATUS.REMOVED]: 'r',
  [CHARGER_STATUS.UNKNOWN]: 'u',
});
const CODE_TO_STATUS = Object.freeze(
  Object.fromEntries(Object.entries(STATUS_TO_CODE).map(([s, c]) => [c, s]))
);

/**
 * FNV-1a over the point-index key list. Identifies "which set of points, in which
 * order" cheaply enough to run every status run (~10 ms for FR's 165k keys).
 * The length prefix makes a collision need both the same count and the same hash.
 */
function pointIndexFingerprint(pointIds) {
  let h = 0x811c9dc5;
  for (const id of pointIds) {
    for (let i = 0; i < id.length; i++) {
      h = Math.imul(h ^ id.charCodeAt(i), 0x01000193);
    }
    h = Math.imul(h ^ 0x2f, 0x01000193); // separator: ["ab","c"] must differ from ["a","bc"]
  }
  return `${pointIds.length}.${(h >>> 0).toString(16)}`;
}

/**
 * Encode the current feed as one character per point, in `pointIds` order.
 * Points the feed did not carry encode as "unknown" — which is exactly how the
 * reader treated a missing entry in the old map shape, so the delta comparison
 * is unchanged.
 */
function encodeLastFeed(pointIds, statusOf) {
  let out = '';
  for (const pointId of pointIds) out += STATUS_TO_CODE[statusOf(pointId)] || 'u';
  return out;
}

/**
 * Build a `(index) => status` reader over a stored snapshot, or null when the
 * snapshot is missing, stale, or of an unusable shape (caller falls back to the
 * full path). Accepts the legacy `{pointId: status}` map so the first run after
 * deploy still takes the cheap path instead of forcing a full MGET-all sweep.
 */
function lastFeedReader(value, pointIds, fingerprint) {
  if (!value || typeof value !== 'object') return null;
  if (value.v === LASTFEED_VERSION) {
    if (value.g !== fingerprint) return null; // point index was rebuilt
    if (typeof value.s !== 'string' || value.s.length !== pointIds.length) return null;
    return (i) => CODE_TO_STATUS[value.s[i]] || CHARGER_STATUS.UNKNOWN;
  }
  if (Number.isInteger(value.__chunks)) return null; // caller must resolve chunks first
  return (i) => value[pointIds[i]] || CHARGER_STATUS.UNKNOWN; // legacy map
}

/** Wrap an encoded snapshot for storage. One plain SET — never chunked. */
function lastFeedPayload(fingerprint, encoded) {
  return JSON.stringify({ v: LASTFEED_VERSION, g: fingerprint, s: encoded });
}

/**
 * True when a stored snapshot already holds exactly this encoding — the write is
 * then pure waste. Quiet runs (night, or a feed that did not move) hit this.
 */
function lastFeedUnchanged(value, fingerprint, encoded) {
  return !!value && value.v === LASTFEED_VERSION && value.g === fingerprint && value.s === encoded;
}

/**
 * Build a Redis client from env, or throw with a clear message.
 * @returns {import('@upstash/redis').Redis}
 */
function makeRedis() {
  const url = process.env.UPSTASH_REDIS_REST_URL;
  const token = process.env.UPSTASH_REDIS_REST_TOKEN;
  if (!url || !token) {
    throw new Error(
      'Missing Upstash credentials: set UPSTASH_REDIS_REST_URL and UPSTASH_REDIS_REST_TOKEN.'
    );
  }
  return new Redis({ url, token });
}

/**
 * @typedef {Object} StoreResult
 * @property {number} upserted   chargers written (or that would be written)
 * @property {number} skipped    chargers skipped (bad coords)
 * @property {number} swept      stale ids removed (0 for status-only / dry-run)
 * @property {boolean} dryRun
 */

/**
 * Write chargers into Upstash. Full refresh: upsert + mark-and-sweep.
 *
 * @param {import('./schema').Charger[]} chargers
 * @param {Object} opts
 * @param {string} opts.country        ISO alpha-2.
 * @param {boolean} [opts.dryRun]      Skip all writes; report only.
 * @param {boolean} [opts.statusOnly]  Update JSON+geo for existing ids but DO NOT sweep
 *                                     (status-only runs don't have the full site set).
 * @param {(msg:string)=>void} [opts.log]
 * @returns {Promise<StoreResult>}
 */
async function writeChargers(chargers, opts) {
  const { country } = opts;
  if (!country) throw new Error('writeChargers: opts.country is required');
  const cc = country.toLowerCase();
  const dryRun = !!opts.dryRun;
  const statusOnly = !!opts.statusOnly;
  const log = opts.log || (() => {});

  let upserted = 0;
  let skipped = 0;
  const currentIds = new Set();

  if (dryRun) {
    for (const c of chargers) {
      if (!isFinite(c.lat) || !isFinite(c.lon)) { skipped++; continue; }
      currentIds.add(String(c.id));
      upserted++;
    }
    log(`[dry-run] would upsert ${upserted} chargers (${skipped} skipped for bad coords)`);
    return { upserted, skipped, swept: 0, dryRun: true };
  }

  const redis = makeRedis();

  // Snapshot previous ids up-front (for the sweep). If this read fails we abort
  // before touching anything.
  let previousIds = new Set();
  if (!statusOnly) {
    const prev = await redis.smembers(IDS_KEY(cc));
    previousIds = new Set((prev || []).map(String));
  }

  // ---- Phase 0: preserve accumulated availability history across full refreshes ----
  // A full refresh overwrites each charger JSON with freshly-normalized data (no
  // history). Carry the existing per-charger `history` profile forward so the
  // hourly accumulator's work isn't wiped. (Full refresh only; status-only never
  // rebuilds the record wholesale.)
  if (!statusOnly) {
    const incoming = chargers.filter((c) => isFinite(c.lat) && isFinite(c.lon));
    for (let i = 0; i < incoming.length; i += PIPELINE_BATCH) {
      const slice = incoming.slice(i, i + PIPELINE_BATCH);
      const existing = await redis.mget(...slice.map((c) => CHARGER_KEY(cc, String(c.id))));
      for (let k = 0; k < slice.length; k++) {
        const raw = existing[k];
        if (!raw) continue;
        const prevRec = typeof raw === 'string' ? JSON.parse(raw) : raw;
        if (!prevRec) continue;
        if (prevRec.history) slice[k].history = prevRec.history;
        // Carry per-connector lastBusyAt ("last used", stamped by the status runs)
        // forward as well — the freshly-normalized record has no memory of past
        // sessions, so without this every full refresh would wipe the timestamps.
        if (Array.isArray(prevRec.connectors)) {
          const busyByPoint = new Map();
          for (const pc of prevRec.connectors) {
            if (pc && pc.pointId && pc.lastBusyAt) busyByPoint.set(String(pc.pointId), pc.lastBusyAt);
          }
          if (busyByPoint.size > 0) {
            for (const nc of slice[k].connectors || []) {
              if (nc && !nc.lastBusyAt && nc.pointId && busyByPoint.has(String(nc.pointId))) {
                nc.lastBusyAt = busyByPoint.get(String(nc.pointId));
              }
            }
          }
        }
      }
    }
  }

  // ---- Phase 1: upsert in batches ----
  // GEOADD accepts many members in one call and MSET writes many keys in one call;
  // Upstash bills each as ONE command (vs one GEOADD + one SET per charger). A full
  // refresh of ~14k chargers thus costs a few hundred commands instead of ~28k.
  let geoMembers = []; // { longitude, latitude, member }
  let jsonUpdates = {}; // CHARGER_KEY -> json string
  const flush = async () => {
    if (geoMembers.length === 0) return;
    await redis.geoadd(GEO_KEY(cc), ...geoMembers);
    await redis.mset(jsonUpdates);
    geoMembers = [];
    jsonUpdates = {};
  };

  for (const c of chargers) {
    if (!isFinite(c.lat) || !isFinite(c.lon)) { skipped++; continue; }
    const id = String(c.id);
    currentIds.add(id);

    geoMembers.push({ longitude: c.lon, latitude: c.lat, member: id });
    jsonUpdates[CHARGER_KEY(cc, id)] = JSON.stringify(c);
    upserted++;

    if (geoMembers.length >= PIPELINE_BATCH) await flush();
  }
  await flush();

  // ---- Phase 2: bookkeeping + mark-and-sweep (full refresh only) ----
  let swept = 0;
  if (!statusOnly) {
    // Replace the id bookkeeping set with the current ids.
    const staleIds = [...previousIds].filter((id) => !currentIds.has(id));

    if (staleIds.length > 0) {
      // Multi-key ZREM + DEL: one command each per batch, not one per stale id.
      for (let i = 0; i < staleIds.length; i += PIPELINE_BATCH) {
        const chunk = staleIds.slice(i, i + PIPELINE_BATCH);
        await redis.zrem(GEO_KEY(cc), ...chunk);
        await redis.del(...chunk.map((id) => CHARGER_KEY(cc, id)));
      }
      swept = staleIds.length;
    }

    // Rewrite the ids set to exactly the current ids.
    await redis.del(IDS_KEY(cc));
    const idArr = [...currentIds];
    for (let i = 0; i < idArr.length; i += PIPELINE_BATCH) {
      await redis.sadd(IDS_KEY(cc), ...idArr.slice(i, i + PIPELINE_BATCH));
    }
  }

  // ---- Phase 3: rebuild the auxiliary status indexes (full refresh only) ----
  // These let the frequent status runs avoid MGET-all: pointindex maps each refill
  // point to its charger (PT/FR diff patch); coordindex gives coords for the ES
  // marker sweep. Stable until the next full refresh. Written via chunkedSet: one
  // SET for PT/ES-sized indexes exactly as before; FR's ~165k-point index exceeds
  // Upstash's ~1 MB request cap and is split transparently (a handful of SETs).
  if (!statusOnly) {
    const pointIndex = {};
    const coordIndex = {};
    for (const c of chargers) {
      if (!isFinite(c.lat) || !isFinite(c.lon)) continue;
      const id = String(c.id);
      coordIndex[id] = [c.lat, c.lon];
      for (const conn of c.connectors || []) {
        if (conn.pointId) pointIndex[String(conn.pointId)] = id;
      }
    }
    await chunkedSet(redis, POINTINDEX_KEY(cc), pointIndex);
    await chunkedSet(redis, COORDINDEX_KEY(cc), coordIndex);
  }

  await redis.set(META_KEY(cc), JSON.stringify({
    lastRun: new Date().toISOString(),
    count: currentIds.size,
    mode: statusOnly ? 'status-only' : 'full',
  }));

  log(`wrote ${upserted} chargers, swept ${swept} stale, skipped ${skipped}`);
  return { upserted, skipped, swept, dryRun: false };
}

module.exports = {
  writeChargers,
  makeRedis,
  chunkedSet,
  chunkedGet,
  chunkedGetFrom,
  pointIndexFingerprint,
  encodeLastFeed,
  lastFeedReader,
  lastFeedPayload,
  lastFeedUnchanged,
  keys: { GEO_KEY, IDS_KEY, META_KEY, CHARGER_KEY, POINTINDEX_KEY, COORDINDEX_KEY, LASTFEED_KEY, CRAWL_KEY, CRAWL_PAGE_KEY },
};
