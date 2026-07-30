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

const PIPELINE_BATCH = 200; // records per pipeline flush

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
        if (prevRec && prevRec.history) slice[k].history = prevRec.history;
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
  // point to its charger (PT diff patch); coordindex gives coords for the ES marker
  // sweep. Stable until the next full refresh. One SET each (~1 command).
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
    await redis.set(POINTINDEX_KEY(cc), JSON.stringify(pointIndex));
    await redis.set(COORDINDEX_KEY(cc), JSON.stringify(coordIndex));
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
  keys: { GEO_KEY, IDS_KEY, META_KEY, CHARGER_KEY, POINTINDEX_KEY, COORDINDEX_KEY, LASTFEED_KEY, CRAWL_KEY, CRAWL_PAGE_KEY },
};
