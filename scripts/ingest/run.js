#!/usr/bin/env node
/**
 * run.js — charger ingest CLI.
 *
 * Usage:
 *   node scripts/ingest/run.js --country=PT [--status-only] [--dry-run]
 *
 * Flags:
 *   --country=<cc>   ISO 3166-1 alpha-2 (default PT). Selects the provider.
 *   --status-only    Refresh only live status (hourly cron). Fetches the small
 *                    status feed, patches connector statuses onto the charger JSON
 *                    already in Redis. No full re-parse, no sweep.
 *   --dry-run        Parse + summarize, but DO NOT write to Upstash.
 *
 * Behavior:
 *   full  = fetch static+status -> normalize -> upsert + mark-and-sweep.
 *   status-only = fetch status -> patch existing Redis records' statuses.
 *
 * Error handling: any fetch/parse/write failure logs and exits non-zero WITHOUT
 * a partial destructive write (store.js only sweeps after a clean upsert phase).
 */

const { getProvider } = require('./providers');
const { writeChargers, makeRedis, keys } = require('./store');
const { CHARGER_STATUS } = require('./schema');

// Availability history: 7 days x 24 hours = 168 UTC buckets, one byte each
// (fraction-available scaled 0-255). Accumulated per status-only run via EWMA.
const HISTORY_BUCKETS = 168;
const HISTORY_ALPHA = 0.25; // weight of the newest observation

/**
 * EWMA-accumulate the current fraction-available into this weekday+hour bucket.
 * Seeds a brand-new profile with the first observation so the slider isn't biased
 * to 0 while data builds. Mutates charger.history (base64). Returns true if written.
 */
function accumulateHistory(charger, now) {
  const conns = charger.connectors || [];
  const total = conns.length;
  if (total === 0) return false;
  const available = conns.filter((c) => c.status === CHARGER_STATUS.AVAILABLE).length;
  const frac = available / total;
  const seed = Math.round(frac * 255);

  let hist;
  if (typeof charger.history === 'string') {
    const buf = Buffer.from(charger.history, 'base64');
    hist = buf.length === HISTORY_BUCKETS ? new Uint8Array(buf) : new Uint8Array(HISTORY_BUCKETS).fill(seed);
  } else {
    hist = new Uint8Array(HISTORY_BUCKETS).fill(seed);
  }
  const bucket = now.getUTCDay() * 24 + now.getUTCHours(); // 0..167
  const prev = hist[bucket] / 255;
  const next = prev * (1 - HISTORY_ALPHA) + frac * HISTORY_ALPHA;
  hist[bucket] = Math.max(0, Math.min(255, Math.round(next * 255)));
  charger.history = Buffer.from(hist).toString('base64');
  return true;
}

function parseArgs(argv) {
  const args = { country: 'PT', statusOnly: false, dryRun: false };
  for (const a of argv.slice(2)) {
    if (a === '--status-only') args.statusOnly = true;
    else if (a === '--dry-run') args.dryRun = true;
    else if (a.startsWith('--country=')) args.country = a.slice('--country='.length).toUpperCase();
    else if (a === '--help' || a === '-h') args.help = true;
    else console.warn(`Unknown argument ignored: ${a}`);
  }
  return args;
}

function log(...m) { console.log('[ingest]', ...m); }
function errlog(...m) { console.error('[ingest]', ...m); }

/** Build a summary of counts (sites, points, by connector type, by status). */
function summarize(chargers) {
  const byConnector = {};
  const byStatus = {};
  let points = 0;
  for (const c of chargers) {
    byStatus[c.status] = (byStatus[c.status] || 0) + 1;
    for (const conn of c.connectors) {
      points++;
      byConnector[conn.standard] = (byConnector[conn.standard] || 0) + 1;
    }
  }
  return { sites: chargers.length, points, byConnector, byStatus };
}

function printSummary(s) {
  log(`sites:  ${s.sites}`);
  log(`points: ${s.points}`);
  log('by connector type:');
  for (const [k, v] of Object.entries(s.byConnector).sort((a, b) => b[1] - a[1])) log(`   ${k.padEnd(10)} ${v}`);
  log('by status:');
  for (const [k, v] of Object.entries(s.byStatus).sort((a, b) => b[1] - a[1])) log(`   ${k.padEnd(12)} ${v}`);
}

/**
 * Resumable full crawl: for providers whose static feed is rate-limited below
 * one-run coverage (e.g. REVE at ~5 req/hr). Each run fetches a small chunk and
 * checkpoints in Redis; the previous dataset keeps serving until the crawl reaches
 * its final page, at which point the whole set is written in one upsert+sweep.
 */
async function runResumableFull(provider, args) {
  const redis = makeRedis();
  log(`resumable full crawl for ${args.country} via provider "${provider.name}"`);
  const crawl = await provider.crawlStatic(redis, { keys, log, dryRun: args.dryRun });

  if (!crawl.complete) {
    log(`crawl not complete this run (next page ${crawl.nextPage}, ${crawl.fetched} page(s) fetched) — no write until the full registry is crawled.`);
    return { upserted: 0, skipped: 0, swept: 0, dryRun: !!args.dryRun, crawl: crawl.dryRun ? 'dry-run' : 'partial', nextPage: crawl.nextPage };
  }

  const chargers = [];
  const { sites, points } = await provider.normalizeStreaming({ locations: crawl.locations }, null, (c) => chargers.push(c));
  log(`parsed ${sites} sites / ${points} points from ${crawl.locations.length} locations`);
  printSummary(summarize(chargers));

  const res = await writeChargers(chargers, { country: args.country, dryRun: false, statusOnly: false, log });
  await provider.clearCrawlState(redis, keys, crawl.lastPage);
  log('crawl cycle complete — checkpoint cleared.');
  return { ...res, crawl: 'complete' };
}

/** Full refresh: fetch static+status, normalize, write. */
async function runFull(provider, args) {
  // Rate-limited providers crawl across runs (checkpointed) instead of one big fetch.
  if (typeof provider.crawlStatic === 'function') return runResumableFull(provider, args);
  log(`full refresh for ${args.country} via provider "${provider.name}"`);
  const { staticStream, statusStream } = await provider.fetch({ statusOnly: false });

  const chargers = [];
  const { sites, points, statusCount } = await provider.normalizeStreaming(
    staticStream,
    statusStream,
    (c) => chargers.push(c)
  );
  log(`parsed ${sites} sites / ${points} points, joined ${statusCount} live statuses`);

  const summary = summarize(chargers);
  printSummary(summary);

  const res = await writeChargers(chargers, {
    country: args.country,
    dryRun: args.dryRun,
    statusOnly: false,
    log,
  });
  return res;
}

/**
 * Status-only refresh: fetch just the status feed, patch existing Redis records.
 * Reads each charger JSON, updates connector + aggregate statuses, writes back.
 * No sweep (we don't have the full site set here).
 */
async function runStatusOnly(provider, args, redisClient) {
  log(`status-only refresh for ${args.country} via provider "${provider.name}"`);

  // Coordinate-based area sweep (e.g. ES/REVE via /markers): the provider owns the
  // whole flow — read station coords, match live markers, accumulate history, write.
  if (typeof provider.runAreaStatus === 'function') {
    const redis = redisClient || makeRedis();
    const res = await provider.runAreaStatus(redis, {
      keys,
      accumulateHistory,
      log,
      dryRun: args.dryRun,
    });
    return {
      upserted: res.upserted || 0,
      skipped: 0,
      swept: 0,
      dryRun: !!args.dryRun,
      statuses: res.statuses || 0,
    };
  }

  // Providers without a live-status feed (e.g. DGT) have nothing to do in
  // status-only mode. Skip gracefully instead of wiping every record to "unknown"
  // — a no-op keeps the last full-refresh data intact.
  if (provider.hasStatusFeed === false) {
    log(`provider "${provider.name}" has no live-status feed — status-only run is a no-op.`);
    return { upserted: 0, skipped: 0, swept: 0, dryRun: args.dryRun, statuses: 0, noop: true };
  }

  const { statusStream } = await provider.fetch({ statusOnly: true });
  const statusById = await provider.parseStatus(statusStream);
  log(`parsed ${statusById.size} live statuses`);

  const statusTally = {};
  for (const { status } of statusById.values()) statusTally[status] = (statusTally[status] || 0) + 1;
  log('status feed breakdown:');
  for (const [k, v] of Object.entries(statusTally).sort((a, b) => b[1] - a[1])) log(`   ${k.padEnd(12)} ${v}`);

  if (args.dryRun) {
    log('[dry-run] would patch statuses onto existing Redis records (no writes)');
    return { upserted: 0, skipped: 0, swept: 0, dryRun: true, statuses: statusById.size };
  }

  const redis = redisClient || makeRedis();
  const cc = args.country.toLowerCase();
  const ids = await redis.smembers(keys.IDS_KEY(cc));
  if (!ids || ids.length === 0) {
    throw new Error(
      `No existing charger ids in Redis for ${args.country}. Run a full refresh first (omit --status-only).`
    );
  }

  const nowTs = new Date();
  const BATCH = 200;
  const statusOf = (pointId) => ((pointId && statusById.get(pointId)) || {}).status || CHARGER_STATUS.UNKNOWN;
  const feedSnapshot = () => {
    const o = {};
    for (const [pointId, v] of statusById.entries()) o[pointId] = (v && v.status) || CHARGER_STATUS.UNKNOWN;
    return o;
  };

  // History is bucketed per UTC weekday+hour, so accumulate at most once per hour —
  // the frequent (e.g. 15-min) runs only patch status. `lastHistoryHour` in meta
  // gates it; whichever run first enters a new hour rolls history for everyone.
  const hourStamp = nowTs.toISOString().slice(0, 13); // "YYYY-MM-DDTHH"
  const meta = parseJson(await redis.get(keys.META_KEY(cc)));
  const accumulate = !meta || meta.lastHistoryHour !== hourStamp;

  // ---- Fast delta path (no history this run): only touch records whose status
  // changed since the previous run. Needs the stable pointindex + the last-feed
  // snapshot; if either is missing we fall through to the full path (always correct). ----
  if (!accumulate) {
    const pointIndex = parseJson(await redis.get(keys.POINTINDEX_KEY(cc)));
    const lastFeed = parseJson(await redis.get(keys.LASTFEED_KEY(cc)));
    if (pointIndex && lastFeed) {
      const changedIds = new Set();
      for (const [pointId, chargerId] of Object.entries(pointIndex)) {
        if (lastFeed[pointId] !== statusOf(pointId)) changedIds.add(chargerId);
      }
      const changed = [...changedIds];
      let patched = 0;
      for (let i = 0; i < changed.length; i += BATCH) {
        const slice = changed.slice(i, i + BATCH);
        const jsons = await redis.mget(...slice.map((id) => keys.CHARGER_KEY(cc, id)));
        const updates = {};
        for (let j = 0; j < slice.length; j++) {
          const charger = parseJson(jsons[j]);
          if (!charger) continue;
          let ch = false;
          for (const conn of charger.connectors || []) {
            const next = statusOf(conn.pointId);
            if (conn.status !== next) {
              // Session start: the poll where a plug first enters "charging" is its
              // real "last used" moment (accurate to the status cadence).
              if (next === CHARGER_STATUS.CHARGING) conn.lastBusyAt = nowTs.toISOString();
              conn.status = next;
              ch = true;
            }
          }
          const agg = aggregateStatus((charger.connectors || []).map((c) => c.status));
          if (charger.status !== agg) { charger.status = agg; ch = true; }
          if (ch) {
            charger.lastUpdated = new Date().toISOString();
            patched++;
            updates[keys.CHARGER_KEY(cc, slice[j])] = JSON.stringify(charger);
          }
        }
        if (Object.keys(updates).length > 0) await redis.mset(updates);
      }
      await redis.set(keys.LASTFEED_KEY(cc), JSON.stringify(feedSnapshot()));
      await redis.set(keys.META_KEY(cc), JSON.stringify({
        lastRun: nowTs.toISOString(),
        count: ids.length,
        mode: 'status-delta',
        lastHistoryHour: meta ? meta.lastHistoryHour : null,
      }));
      log(`delta: patched status on ${patched} record(s) (${changed.length} candidates, ${ids.length} total)`);
      return { upserted: patched, skipped: 0, swept: 0, dryRun: false, statuses: statusById.size };
    }
  }

  // ---- Full path: MGET-all, patch status, roll history when this is the hour's
  // first run, MSET only changed. Rebuilds the last-feed snapshot for the delta path. ----
  let patched = 0;
  for (let i = 0; i < ids.length; i += BATCH) {
    const slice = ids.slice(i, i + BATCH);
    const jsons = await redis.mget(...slice.map((id) => keys.CHARGER_KEY(cc, id)));

    const updates = {}; // key -> JSON string, flushed in a single MSET
    for (let j = 0; j < slice.length; j++) {
      const charger = parseJson(jsons[j]);
      if (!charger) continue;
      let changed = false;
      for (const conn of charger.connectors || []) {
        const next = statusOf(conn.pointId);
        if (conn.status !== next) {
          // Session start = "last used" (see delta path above).
          if (next === CHARGER_STATUS.CHARGING) conn.lastBusyAt = nowTs.toISOString();
          conn.status = next;
          changed = true;
        }
      }
      const agg = aggregateStatus((charger.connectors || []).map((c) => c.status));
      if (charger.status !== agg) { charger.status = agg; changed = true; }

      // Roll the availability observation into the hourly history profile (gated).
      const histChanged = accumulate ? accumulateHistory(charger, nowTs) : false;

      if (changed) { charger.lastUpdated = new Date().toISOString(); patched++; }
      if (changed || histChanged) {
        updates[keys.CHARGER_KEY(cc, slice[j])] = JSON.stringify(charger);
      }
    }
    if (Object.keys(updates).length > 0) await redis.mset(updates);
  }

  await redis.set(keys.LASTFEED_KEY(cc), JSON.stringify(feedSnapshot()));
  await redis.set(keys.META_KEY(cc), JSON.stringify({
    lastRun: nowTs.toISOString(),
    count: ids.length,
    mode: 'status-only',
    lastHistoryHour: accumulate ? hourStamp : (meta ? meta.lastHistoryHour : null),
  }));

  log(`patched status on ${patched} of ${ids.length} records${accumulate ? ' (history rolled)' : ''}`);
  return { upserted: patched, skipped: 0, swept: 0, dryRun: false, statuses: statusById.size };
}

/** Parse a value that may be a JSON string or an already-deserialized object. */
function parseJson(raw) {
  if (raw == null) return null;
  if (typeof raw === 'string') { try { return JSON.parse(raw); } catch (_e) { return null; } }
  return raw;
}

/** Mirror of provider aggregateStatus (kept local to avoid exporting internals). */
function aggregateStatus(statuses) {
  if (!statuses.length) return CHARGER_STATUS.UNKNOWN;
  if (statuses.includes(CHARGER_STATUS.AVAILABLE)) return CHARGER_STATUS.AVAILABLE;
  if (statuses.includes(CHARGER_STATUS.CHARGING)) return CHARGER_STATUS.CHARGING;
  if (statuses.every((s) => s === CHARGER_STATUS.OUT_OF_ORDER)) return CHARGER_STATUS.OUT_OF_ORDER;
  if (statuses.every((s) => s === CHARGER_STATUS.PLANNED)) return CHARGER_STATUS.PLANNED;
  if (statuses.every((s) => s === CHARGER_STATUS.REMOVED)) return CHARGER_STATUS.REMOVED;
  return statuses.find((s) => s !== CHARGER_STATUS.UNKNOWN) || CHARGER_STATUS.UNKNOWN;
}

async function main() {
  const args = parseArgs(process.argv);
  if (args.help) {
    console.log('Usage: node scripts/ingest/run.js --country=PT [--status-only] [--dry-run]');
    process.exit(0);
  }

  log(`country=${args.country} statusOnly=${args.statusOnly} dryRun=${args.dryRun}`);
  const started = Date.now();

  let provider;
  try {
    provider = getProvider(args.country);
  } catch (e) {
    errlog('provider resolution failed:', e.message);
    process.exit(2);
  }

  try {
    const res = args.statusOnly
      ? await runStatusOnly(provider, args)
      : await runFull(provider, args);
    log(`done in ${((Date.now() - started) / 1000).toFixed(1)}s`, JSON.stringify(res));
    process.exit(0);
  } catch (e) {
    errlog('FAILED — aborting without destructive write:', e.stack || e.message);
    process.exit(1);
  }
}

if (require.main === module) main();

module.exports = { runStatusOnly, accumulateHistory, aggregateStatus };
