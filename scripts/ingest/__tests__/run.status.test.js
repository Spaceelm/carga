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

const { runStatusOnly } = require('../run');
const { keys } = require('../store');
const { CHARGER_STATUS } = require('../schema');

const CC = 'pt';
const hourStamp = () => new Date().toISOString().slice(0, 13);

function makeFakeRedis(seed) {
  const store = new Map(Object.entries(seed));
  const calls = { smembers: 0, mget: 0, mgetKeys: 0, mset: 0, get: 0, set: 0 };
  return {
    _store: store,
    _calls: calls,
    async smembers(k) { calls.smembers++; return store.get(k) || []; },
    async mget(...ks) { calls.mget++; calls.mgetKeys += ks.length; return ks.map((k) => (store.has(k) ? store.get(k) : null)); },
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
  assert.strictEqual(redis._calls.mgetKeys, 1, 'MGET only the changed charger (not all)');
  const b = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'B')));
  assert.strictEqual(b.connectors[0].status, CHARGER_STATUS.CHARGING);
  const a = JSON.parse(redis._store.get(keys.CHARGER_KEY(CC, 'A')));
  assert.strictEqual(a.connectors[0].status, CHARGER_STATUS.AVAILABLE, 'unchanged charger untouched');
  assert.ok(!a.history, 'no history rolled on the delta path');
  // last-feed refreshed for the next run.
  assert.deepStrictEqual(JSON.parse(redis._store.get(keys.LASTFEED_KEY(CC))), { pA: CHARGER_STATUS.AVAILABLE, pB: CHARGER_STATUS.CHARGING });
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

  assert.strictEqual(redis._calls.mgetKeys, 2, 'MGET all records on the history run');
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
