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

const GEO_KEY = (cc) => `chargers:${cc.toLowerCase()}:geo`;
const IDS_KEY = (cc) => `chargers:${cc.toLowerCase()}:ids`;
const META_KEY = (cc) => `chargers:${cc.toLowerCase()}:meta`;
const CHARGER_KEY = (cc, id) => `charger:${cc.toLowerCase()}:${id}`;
// Auxiliary indexes for the cheap status path (written at full refresh; stable
// between refreshes). pointindex: refill-point id -> charger id (PT status diff).
// coordindex: charger id -> [lat, lon] (ES marker-match sweep without MGET-all).
// lastfeed: previous status feed (pointId -> status) — a delta cache, safe if lost.
const POINTINDEX_KEY = (cc) => `chargers:${cc.toLowerCase()}:pointindex`;
const COORDINDEX_KEY = (cc) => `chargers:${cc.toLowerCase()}:coordindex`;
const LASTFEED_KEY = (cc) => `chargers:${cc.toLowerCase()}:lastfeed`;
// Resumable full-crawl checkpoint (providers rate-limited below one-run coverage,
// e.g. REVE at ~5 req/hr). crawl: JSON { nextPage, startedAt }; each fetched page is
// staged under crawl:page:<n> until the final page is reached, then a single
// upsert+sweep consumes them (see providers/reve.js crawlStatic).
const CRAWL_KEY = (cc) => `chargers:${cc.toLowerCase()}:crawl`;
const CRAWL_PAGE_KEY = (cc, n) => `chargers:${cc.toLowerCase()}:crawl:page:${n}`;

// ---- Batch sizing (the Upstash command budget) -------------------------------
// Upstash bills a multi-key command (MGET, MSET, GEOADD, SADD, ZREM, DEL) as ONE
// command no matter how many keys it carries; what's capped is the ~1 MB REQUEST
// body. Key-only commands (MGET/DEL/ZREM/SADD/GEOADD) carry tiny bodies, so they
// batch at KEY_BATCH. MSET carries the record JSON, so it flushes by request size
// (see makeMsetWriter) instead of a fixed record count. Both are env-tunable in
// case a provider's records grow (or a response-size limit ever bites).
// Previously everything batched at 200, which made batch COUNT — not data — the
// dominant cost of every history roll and full refresh.
const KEY_BATCH = Math.max(1, parseInt(process.env.INGEST_KEY_BATCH || '', 10) || 1000);
// Measured on the doubly-escaped size (value JSON-encoded again inside the REST
// command), same reasoning as CHUNK_CHARS below. 700k leaves headroom under ~1 MB.
const MSET_MAX_BYTES = Math.max(1000, parseInt(process.env.INGEST_MSET_MAX_BYTES || '', 10) || 700000);

/**
 * Accumulates key->string pairs and issues one MSET whenever the next pair would
 * push the request body past MSET_MAX_BYTES. A single oversized record is still
 * sent on its own. Call flush() at the end.
 */
function makeMsetWriter(redis, maxBytes = MSET_MAX_BYTES) {
  let buf = {};
  let bytes = 0;
  let n = 0;
  let written = 0;
  const flush = async () => {
    if (n === 0) return;
    await redis.mset(buf);
    written += n;
    buf = {};
    bytes = 0;
    n = 0;
  };
  const add = async (key, value) => {
    const sz = JSON.stringify(key).length + JSON.stringify(value).length + 2;
    if (n > 0 && bytes + sz > maxBytes) await flush();
    buf[key] = value;
    bytes += sz;
    n++;
  };
  return { add, flush, get written() { return written; } };
}

// ---- Usage accounting -------------------------------------------------------
// Every client built by makeRedis() is wrapped so each run can report exactly how
// many Upstash commands it spent and roughly how many bytes it moved (request
// args + response, JSON-serialized — close to what the REST API transfers).
const stats = { commands: 0, bytesOut: 0, bytesIn: 0, byCommand: {} };

function approxBytes(v) {
  if (v == null) return 0;
  if (typeof v === 'string') return v.length;
  try { return JSON.stringify(v).length; } catch (_e) { return 0; }
}

function instrument(client) {
  return new Proxy(client, {
    get(target, prop, receiver) {
      const val = Reflect.get(target, prop, receiver);
      if (typeof val !== 'function' || typeof prop !== 'string' || prop.startsWith('_')) return val;
      // Only plain command methods are counted; pipelines/multi are not used here.
      if (prop === 'pipeline' || prop === 'multi' || prop === 'constructor') return val.bind(target);
      return async (...args) => {
        stats.commands++;
        stats.byCommand[prop] = (stats.byCommand[prop] || 0) + 1;
        stats.bytesOut += approxBytes(args);
        const res = await val.apply(target, args);
        stats.bytesIn += approxBytes(res);
        return res;
      };
    },
  });
}

function redisStats() {
  return { ...stats, byCommand: { ...stats.byCommand } };
}

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
  const base = await redis.get(key);
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
  return instrument(new Redis({ url, token }));
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
    for (let i = 0; i < incoming.length; i += KEY_BATCH) {
      const slice = incoming.slice(i, i + KEY_BATCH);
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
  // refresh of ~14k chargers thus costs a few dozen commands instead of ~28k.
  // GEOADD batches by member count (tiny bodies); MSET flushes by request size.
  let geoMembers = []; // { longitude, latitude, member }
  const jsonWriter = makeMsetWriter(redis);
  const flushGeo = async () => {
    if (geoMembers.length === 0) return;
    await redis.geoadd(GEO_KEY(cc), ...geoMembers);
    geoMembers = [];
  };

  for (const c of chargers) {
    if (!isFinite(c.lat) || !isFinite(c.lon)) { skipped++; continue; }
    const id = String(c.id);
    currentIds.add(id);

    geoMembers.push({ longitude: c.lon, latitude: c.lat, member: id });
    await jsonWriter.add(CHARGER_KEY(cc, id), JSON.stringify(c));
    upserted++;

    if (geoMembers.length >= KEY_BATCH) await flushGeo();
  }
  await flushGeo();
  await jsonWriter.flush();

  // ---- Phase 2: bookkeeping + mark-and-sweep (full refresh only) ----
  let swept = 0;
  if (!statusOnly) {
    // Replace the id bookkeeping set with the current ids.
    const staleIds = [...previousIds].filter((id) => !currentIds.has(id));

    if (staleIds.length > 0) {
      // Multi-key ZREM + DEL: one command each per batch, not one per stale id.
      for (let i = 0; i < staleIds.length; i += KEY_BATCH) {
        const chunk = staleIds.slice(i, i + KEY_BATCH);
        await redis.zrem(GEO_KEY(cc), ...chunk);
        await redis.del(...chunk.map((id) => CHARGER_KEY(cc, id)));
      }
      swept = staleIds.length;
    }

    // Rewrite the ids set to exactly the current ids.
    await redis.del(IDS_KEY(cc));
    const idArr = [...currentIds];
    for (let i = 0; i < idArr.length; i += KEY_BATCH) {
      await redis.sadd(IDS_KEY(cc), ...idArr.slice(i, i + KEY_BATCH));
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
  makeMsetWriter,
  redisStats,
  instrument,
  KEY_BATCH,
  keys: { GEO_KEY, IDS_KEY, META_KEY, CHARGER_KEY, POINTINDEX_KEY, COORDINDEX_KEY, LASTFEED_KEY, CRAWL_KEY, CRAWL_PAGE_KEY },
};
