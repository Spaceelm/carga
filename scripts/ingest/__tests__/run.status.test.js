/**
 * Unit tests for the minimal PT status path in run.js (runStatusOnly).
 * Run with:  node --test scripts/ingest/__tests__/run.status.test.js
 *
 * Uses an in-memory fake Redis (injected) to assert:
 *   - delta path: only the status-CHANGED charger is read + written (no MGET-all),
 *     and history is NOT rolled when this hour already accumulated.
 *   - full path: MGET-all, status patched, history rolled once per new hour, and
 *     lastHistoryHour recorded so the next same-hour run takes the delta path.
 */

const test = require('node:test');
const assert = require('node:assert');

const { runStatusOnly, accumulateHistory } = require('../run');
const {
  keys,
  chunkedSet,
  chunkedGet,
  pointIndexFingerprint,
  encodeLastFeed,
  lastFeedReader,
  lastFeedPayload,
} = require('../store');
const { CHARGER_STATUS } = require('../schema');

const CC = 'pt';
const hourStamp = () => new Date().toISOString().slice(0, 13);

function makeFakeRedis(seed) {
  const store = new Map(Object.entries(seed));
  // `recordMgetKeys` counts only per-charger record keys. The status path also
  // MGETs its index keys (meta + pointindex + lastfeed in one round trip), and
  // those must not be confused with "did we sweep every charger record?" — which
  // is the property these tests actually guard.
  const calls = { smembers: 0, mget: 0, mgetKeys: 0, recordMgetKeys: 0, mset: 0, get: 0, set: 0 };
  const isRecordKey = (k) => String(k).startsWith(`charger:${CC}:`);
  return {
    _store: store,
    _calls: calls,
    async smembers(k) { calls.smembers++; return store.get(k) || []; },
    async mget(...ks) {
      calls.mget++;
      calls.mgetKeys += ks.length;
      calls.recordMgetKeys += ks.filter(isRecordKey).length;
      return ks.map((k) => (store.has(k) ? store.get(k) : null));
    },
    async mset(obj) { calls.mset++; for (const [k, v] of Object.entries(obj)) store.set(k, v); },
    async get(k) { calls.get++; return store.has(k) ? store.get(k) : null; },
    async set(k, v) { calls.set++; store.set(k, v); },
  };
}

function rec(id, pointId, status) {
  return JSON.stringify({ id, lat: 40 + Math.random() * 0.001, lon: -3, status, connectors: [{ pointId, status }] });
}

// Provider stub: parseStatus returns the given pointId->status map.
function provider(feed) {
  return {
    name: 'mobie',
    hasStatusFeed: true,
    async fetch() { return { statusStream: null }; },
    async parseStatus() { return new Map(Object.entries(feed).map(([p, s]) => [p, { status: s }])); },
  };
}

test('delta path: only the changed charger is touched, no history roll', async () => {
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['A', 'B'],
    [keys.CHARGER_KEY(CC, 'A')]: rec('A', 'pA', CHARGER_STATUS.AVAILABLE),
    [keys.CHARGER_KEY(CC, 'B')]: rec('B', 'pB', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A', pB: 'B' }),
    [keys.LASTFEED_KEY(CC)]: JSON.stringify({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.AVAILABLE }),
    // Same hour already accumulated -> delta path.
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: hourStamp() }),
  });

  // pB flips to charging; pA unchanged.
  const res = await runStatusOnly(provider({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.CHARGING }), { country: 'PT', dryRun: false }, redis);

  assert.strictEqual(res.upserted, 1, 'only one charger patched');
  assert.strictEqual(redis._calls.recordMgetKeys, 1, 'MGET only the changed charger (not all)');
  assert.strictEqual(redis._calls.smembers, 0, 'delta path does not read the ids set');
  const b = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'B')));
  assert.strictEqual(b.connectors[0].status, CHARGER_STATUS.CHARGING);
  const a = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'A')));
  assert.strictEqual(a.connectors[0].status, CHARGER_STATUS.AVAILABLE, 'unchanged charger untouched');
  assert.ok(!a.history, 'no history rolled on the delta path');
  // Last-feed refreshed for the next run, in the compact positional encoding:
  // one char per point in point-index order ("a"vailable, "c"harging). The seed
  // above is the LEGACY {pointId: status} map, so this also covers reading the
  // old shape and upgrading it in place.
  const snap = JSON.parse(redis._store.get(keys.LASTFEED_KEY(CC)));
  assert.strictEqual(snap.v, 2, 'written in the compact format');
  assert.strictEqual(snap.s, 'ac', 'pA available, pB charging, in point-index order');
  assert.strictEqual(snap.g, pointIndexFingerprint(['pA', 'pB']), 'stamped with the point-index fingerprint');
});

test('full path: MGET-all, history rolled once, lastHistoryHour recorded', async () => {
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['A', 'B'],
    [keys.CHARGER_KEY(CC, 'A')]: rec('A', 'pA', CHARGER_STATUS.AVAILABLE),
    [keys.CHARGER_KEY(CC, 'B')]: rec('B', 'pB', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A', pB: 'B' }),
    // Different hour -> accumulate=true -> full path (history rolled).
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: '2000-01-01T00' }),
  });

  await runStatusOnly(provider({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.AVAILABLE }), { country: 'PT', dryRun: false }, redis);

  assert.strictEqual(redis._calls.recordMgetKeys, 2, 'MGET all records on the history run');
  const a = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'A')));
  assert.ok(typeof a.history === 'string' && a.history.length > 0, 'history rolled');
  const meta = JSON.parse(redis._store.get(keys.META_KEY(CC)));
  assert.strictEqual(meta.lastHistoryHour, hourStamp(), 'lastHistoryHour advanced to this hour');
});

test('transition to charging stamps lastBusyAt (session start); unchanged plugs are not stamped', async () => {
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['A', 'B'],
    [keys.CHARGER_KEY(CC, 'A')]: rec('A', 'pA', CHARGER_STATUS.AVAILABLE),
    [keys.CHARGER_KEY(CC, 'B')]: rec('B', 'pB', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A', pB: 'B' }),
    [keys.LASTFEED_KEY(CC)]: JSON.stringify({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.AVAILABLE }),
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: hourStamp() }),
  });

  await runStatusOnly(provider({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.CHARGING }), { country: 'PT', dryRun: false }, redis);

  const b = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'B')));
  const busyAt = new Date(b.connectors[0].lastBusyAt);
  assert.ok(!isNaN(busyAt.getTime()), 'lastBusyAt is a valid timestamp');
  assert.ok(Date.now() - busyAt.getTime() < 60000, 'lastBusyAt is ~now');
  const a = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'A')));
  assert.ok(!a.connectors[0].lastBusyAt, 'unchanged plug is not stamped');
});

test('transition to a non-charging status does not stamp lastBusyAt', async () => {
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['B'],
    [keys.CHARGER_KEY(CC, 'B')]: rec('B', 'pB', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pB: 'B' }),
    [keys.LASTFEED_KEY(CC)]: JSON.stringify({ pB: CHARGER_STATUS.AVAILABLE }),
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: hourStamp() }),
  });

  await runStatusOnly(provider({ pB: CHARGER_STATUS.OUT_OF_ORDER }), { country: 'PT', dryRun: false }, redis);

  const b = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'B')));
  assert.strictEqual(b.connectors[0].status, CHARGER_STATUS.OUT_OF_ORDER, 'status changed');
  assert.ok(!b.connectors[0].lastBusyAt, 'no session-start stamp for a non-charging transition');
});

// ---------------------------------------------------------------------------
// Chunked index values (FR's point index exceeds Upstash's ~1 MB request cap)
// ---------------------------------------------------------------------------
test('chunkedSet/chunkedGet: small values stay plain (PT/ES unchanged)', async () => {
  const redis = makeFakeRedis({});
  const obj = { a: 'A', b: 'B' };
  const cmds = await chunkedSet(redis, 'k', obj);
  assert.strictEqual(cmds, 1, 'one SET, no chunking');
  assert.strictEqual(redis._store.get('k'), JSON.stringify(obj), 'stored verbatim as before');
  assert.ok(!redis._store.has('k:c0'), 'no chunk keys created');
  assert.deepStrictEqual(await chunkedGet(redis, 'k'), obj);
});

test('chunkedGet reads a legacy plain value written by a bare SET', async () => {
  const redis = makeFakeRedis({ k: JSON.stringify({ p1: 'A' }) });
  assert.deepStrictEqual(await chunkedGet(redis, 'k'), { p1: 'A' });
});

test('chunkedSet/chunkedGet: large values round-trip via chunks', async () => {
  const redis = makeFakeRedis({});
  process.env.INGEST_CHUNK_CHARS = '';
  // Build an index far larger than the chunk threshold used in production.
  const big = {};
  for (let i = 0; i < 60000; i++) big[`FRPDCE${i}`] = `FRSTATION${i % 9000}`;
  const json = JSON.stringify(big);
  assert.ok(json.length > 700000, `fixture must exceed the threshold (was ${json.length})`);
  const cmds = await chunkedSet(redis, 'idx', big);
  assert.ok(cmds > 1, 'chunked into multiple SETs');
  const sentinel = JSON.parse(redis._store.get('idx'));
  assert.ok(Number.isInteger(sentinel.__chunks) && sentinel.__chunks === cmds - 1);
  assert.deepStrictEqual(await chunkedGet(redis, 'idx'), big, 'exact round-trip');
});

test('chunkedGet returns null when a chunk is missing (caller falls back)', async () => {
  const redis = makeFakeRedis({});
  const big = {};
  for (let i = 0; i < 60000; i++) big[`FRPDCE${i}`] = `S${i}`;
  await chunkedSet(redis, 'idx', big);
  redis._store.delete('idx:c1');
  assert.strictEqual(await chunkedGet(redis, 'idx'), null, 'incomplete set is not partially trusted');
});

test('chunkedGet survives a client that pre-deserializes chunk strings', async () => {
  // The Upstash client may JSON-parse values on read; chunks are JSON-encoded
  // strings, so both the raw and the already-unwrapped form must work.
  const redis = makeFakeRedis({});
  const big = {};
  for (let i = 0; i < 60000; i++) big[`FRPDCE${i}`] = `S${i}`;
  await chunkedSet(redis, 'idx', big);
  const n = JSON.parse(redis._store.get('idx')).__chunks;
  for (let k = 0; k < n; k++) {
    const key = `idx:c${k}`;
    redis._store.set(key, JSON.parse(redis._store.get(key))); // simulate auto-deserialization
  }
  assert.deepStrictEqual(await chunkedGet(redis, 'idx'), big);
});

// ---------------------------------------------------------------------------
// History accumulation guards
// ---------------------------------------------------------------------------
test('accumulateHistory skips a charger whose every connector is unknown', () => {
  const c = { connectors: [{ status: CHARGER_STATUS.UNKNOWN }, { status: CHARGER_STATUS.UNKNOWN }] };
  assert.strictEqual(accumulateHistory(c, new Date()), false, 'no observation -> no write');
  assert.strictEqual(c.history, undefined, 'must not seed a fake 0%-available profile');
});

test('accumulateHistory records a partial observation when some plugs are known', () => {
  const c = { connectors: [{ status: CHARGER_STATUS.AVAILABLE }, { status: CHARGER_STATUS.UNKNOWN }] };
  assert.strictEqual(accumulateHistory(c, new Date()), true);
  assert.strictEqual(Buffer.from(c.history, 'base64').length, 168);
});

test('accumulateHistory spreadHours fills the whole window, not just one bucket', () => {
  const at = new Date(Date.UTC(2026, 6, 31, 10, 0, 0)); // Friday 10:00Z
  const c = { connectors: [{ status: CHARGER_STATUS.AVAILABLE }] };
  assert.strictEqual(accumulateHistory(c, at, 3), true);
  const h = Buffer.from(c.history, 'base64');
  const base = at.getUTCDay() * 24;
  for (const hour of [10, 9, 8]) {
    assert.ok(h[base + hour] > 0, `bucket for ${hour}:00 written`);
  }
  // A fresh profile is seeded everywhere, so compare against a 1-hour spread
  // to prove the neighbouring buckets got a second, independent observation.
  const c1 = { connectors: [{ status: CHARGER_STATUS.AVAILABLE }] };
  accumulateHistory(c1, at, 1);
  const h1 = Buffer.from(c1.history, 'base64');
  assert.ok(h[base + 9] >= h1[base + 9], 'spread window rolls the earlier bucket too');
});

test('missing lastfeed falls back to the full path (still correct)', async () => {
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['A'],
    [keys.CHARGER_KEY(CC, 'A')]: rec('A', 'pA', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A' }),
    // Same hour (delta desired) BUT no lastfeed -> must fall back to full path.
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: hourStamp() }),
  });

  await runStatusOnly(provider({ pA: CHARGER_STATUS.CHARGING }), { country: 'PT', dryRun: false }, redis);

  const a = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'A')));
  assert.strictEqual(a.connectors[0].status, CHARGER_STATUS.CHARGING, 'status still patched via full fallback');
  assert.ok(redis._store.get(keys.LASTFEED_KEY(CC)), 'lastfeed seeded for next run');
});

// ---------------------------------------------------------------------------
// Compact last-feed snapshot (store.js): the encoding that keeps a status run's
// fixed cost at a couple of Redis commands regardless of feed size.
// ---------------------------------------------------------------------------

test('lastfeed codec: round-trips every status, one char per point', () => {
  const pointIds = ['p1', 'p2', 'p3', 'p4', 'p5', 'p6'];
  const feed = {
    p1: CHARGER_STATUS.AVAILABLE,
    p2: CHARGER_STATUS.CHARGING,
    p3: CHARGER_STATUS.OUT_OF_ORDER,
    p4: CHARGER_STATUS.PLANNED,
    p5: CHARGER_STATUS.REMOVED,
    p6: CHARGER_STATUS.UNKNOWN,
  };
  const encoded = encodeLastFeed(pointIds, (p) => feed[p]);
  assert.strictEqual(encoded.length, pointIds.length, 'exactly one character per point');

  const gen = pointIndexFingerprint(pointIds);
  const read = lastFeedReader({ v: 2, g: gen, s: encoded }, pointIds, gen);
  assert.ok(read, 'snapshot is readable');
  pointIds.forEach((p, i) => assert.strictEqual(read(i), feed[p], `${p} round-trips`));
});

test('lastfeed codec: a point the feed omits round-trips as unknown', () => {
  const pointIds = ['p1', 'p2'];
  // p2 carries no row at all — the reader must see "unknown", not undefined,
  // or the delta compare would flag it as changed on every single run.
  const encoded = encodeLastFeed(pointIds, (p) => (p === 'p1' ? CHARGER_STATUS.AVAILABLE : undefined));
  const gen = pointIndexFingerprint(pointIds);
  const read = lastFeedReader({ v: 2, g: gen, s: encoded }, pointIds, gen);
  assert.strictEqual(read(1), CHARGER_STATUS.UNKNOWN);
});

test('lastfeed fingerprint: order and membership both matter', () => {
  const a = pointIndexFingerprint(['p1', 'p2']);
  assert.notStrictEqual(a, pointIndexFingerprint(['p2', 'p1']), 'reordering invalidates');
  assert.notStrictEqual(a, pointIndexFingerprint(['p1', 'p3']), 'different members invalidate');
  assert.notStrictEqual(a, pointIndexFingerprint(['p1', 'p2', 'p3']), 'extra member invalidates');
  // Concatenation ambiguity: ["ab","c"] must not hash like ["a","bc"].
  assert.notStrictEqual(pointIndexFingerprint(['ab', 'c']), pointIndexFingerprint(['a', 'bc']));
  assert.strictEqual(a, pointIndexFingerprint(['p1', 'p2']), 'stable for the same list');
});

test('lastfeed reader rejects a snapshot from a different point index', () => {
  const pointIds = ['p1', 'p2'];
  const stale = { v: 2, g: pointIndexFingerprint(['p1', 'p2', 'p3']), s: 'aaa' };
  assert.strictEqual(lastFeedReader(stale, pointIds, pointIndexFingerprint(pointIds)), null);
  // Right fingerprint, wrong length is also unusable.
  const gen = pointIndexFingerprint(pointIds);
  assert.strictEqual(lastFeedReader({ v: 2, g: gen, s: 'a' }, pointIds, gen), null);
});

test('a full refresh (new point index) forces the full path, not a garbled delta', async () => {
  // Snapshot was written against a 2-point index; the full refresh since then
  // rebuilt it with 3 points. Decoding positionally would misread every status,
  // so the stale fingerprint must send the run down the full path instead.
  const stalePayload = lastFeedPayload(pointIndexFingerprint(['pA', 'pB']), 'aa');
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['A', 'B', 'C'],
    [keys.CHARGER_KEY(CC, 'A')]: rec('A', 'pA', CHARGER_STATUS.AVAILABLE),
    [keys.CHARGER_KEY(CC, 'B')]: rec('B', 'pB', CHARGER_STATUS.AVAILABLE),
    [keys.CHARGER_KEY(CC, 'C')]: rec('C', 'pC', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A', pB: 'B', pC: 'C' }),
    [keys.LASTFEED_KEY(CC)]: stalePayload,
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: hourStamp() }),
  });

  await runStatusOnly(
    provider({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.AVAILABLE, pC: CHARGER_STATUS.CHARGING }),
    { country: 'PT', dryRun: false },
    redis
  );

  assert.strictEqual(redis._calls.recordMgetKeys, 3, 'fell back to the full sweep');
  const c = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'C')));
  assert.strictEqual(c.connectors[0].status, CHARGER_STATUS.CHARGING, 'status correct despite the stale snapshot');
  const snap = JSON.parse(redis._store.get(keys.LASTFEED_KEY(CC)));
  assert.strictEqual(snap.g, pointIndexFingerprint(['pA', 'pB', 'pC']), 'snapshot re-stamped for the new index');
  assert.strictEqual(snap.s, 'aac');
});

test('a quiet run writes no last-feed snapshot at all', async () => {
  const gen = pointIndexFingerprint(['pA', 'pB']);
  const redis = makeFakeRedis({
    [keys.IDS_KEY(CC)]: ['A', 'B'],
    [keys.CHARGER_KEY(CC, 'A')]: rec('A', 'pA', CHARGER_STATUS.AVAILABLE),
    [keys.CHARGER_KEY(CC, 'B')]: rec('B', 'pB', CHARGER_STATUS.AVAILABLE),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A', pB: 'B' }),
    [keys.LASTFEED_KEY(CC)]: lastFeedPayload(gen, 'aa'),
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: hourStamp() }),
  });

  // Feed identical to the snapshot -> nothing to patch, nothing to re-store.
  await runStatusOnly(
    provider({ pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.AVAILABLE }),
    { country: 'PT', dryRun: false },
    redis
  );

  assert.strictEqual(redis._calls.recordMgetKeys, 0, 'no charger record read');
  assert.strictEqual(redis._calls.mset, 0, 'no charger record written');
  // One SET only: the meta timestamp. The snapshot is byte-identical, so re-storing
  // it would be pure spend — at a 10-minute cadence that is ~4.4k wasted
  // commands/month per country.
  assert.strictEqual(redis._calls.set, 1, 'meta only; snapshot not rewritten');
});
