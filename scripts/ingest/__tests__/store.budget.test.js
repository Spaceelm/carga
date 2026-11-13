/**
 * Tests for the Upstash budget helpers in store.js:
 *   - makeMsetWriter flushes by request size, not record count
 *   - instrument() counts commands and bytes
 *   - the delta status path uses SCARD, never SMEMBERS
 * Run with:  node --test scripts/ingest/__tests__/store.budget.test.js
 */

const test = require('node:test');
const assert = require('node:assert');

const { makeMsetWriter, instrument, redisStats, keys } = require('../store');
const { runStatusOnly } = require('../run');
const { CHARGER_STATUS } = require('../schema');

function fakeMset() {
  const calls = [];
  return { calls, async mset(obj) { calls.push(Object.keys(obj).length); } };
}

test('makeMsetWriter batches many small records into one MSET', async () => {
  const r = fakeMset();
  const w = makeMsetWriter(r, 700000);
  for (let i = 0; i < 1500; i++) await w.add(`charger:pt:${i}`, JSON.stringify({ id: i, s: 'AVAILABLE' }));
  await w.flush();
  assert.deepStrictEqual(r.calls, [1500]);
  assert.strictEqual(w.written, 1500);
});

test('makeMsetWriter splits when the request body would exceed the cap', async () => {
  const r = fakeMset();
  const w = makeMsetWriter(r, 10000);
  const big = JSON.stringify({ blob: 'x'.repeat(3000) }); // ~3 KB each
  for (let i = 0; i < 10; i++) await w.add(`k${i}`, big);
  await w.flush();
  assert.ok(r.calls.length > 1, 'more than one MSET');
  assert.strictEqual(r.calls.reduce((a, b) => a + b, 0), 10, 'every record written once');
  for (const n of r.calls) assert.ok(n <= 3, 'each MSET stays under the byte cap');
});

test('makeMsetWriter sends an oversized single record on its own', async () => {
  const r = fakeMset();
  const w = makeMsetWriter(r, 100);
  await w.add('a', 'y'.repeat(500));
  await w.add('b', 'z');
  await w.flush();
  assert.deepStrictEqual(r.calls, [1, 1]);
});

test('makeMsetWriter flush on empty buffer issues no command', async () => {
  const r = fakeMset();
  const w = makeMsetWriter(r);
  await w.flush();
  assert.deepStrictEqual(r.calls, []);
});

test('instrument counts commands and bytes per command', async () => {
  const before = redisStats();
  const client = instrument({
    async get() { return 'hello'; },
    async mget(...ks) { return ks.map(() => 'v'); },
  });
  await client.get('k');
  await client.mget('a', 'b', 'c');
  const after = redisStats();
  assert.strictEqual(after.commands - before.commands, 2);
  assert.strictEqual((after.byCommand.get || 0) - (before.byCommand.get || 0), 1);
  assert.strictEqual((after.byCommand.mget || 0) - (before.byCommand.mget || 0), 1);
  assert.ok(after.bytesIn > before.bytesIn);
  assert.ok(after.bytesOut > before.bytesOut);
});

test('delta status path uses SCARD and never SMEMBERS', async () => {
  const CC = 'pt';
  const store = new Map(Object.entries({
    [keys.IDS_KEY(CC)]: ['A'],
    [keys.CHARGER_KEY(CC, 'A')]: JSON.stringify({ id: 'A', lat: 40, lon: -3, status: 'AVAILABLE', connectors: [{ pointId: 'pA', status: CHARGER_STATUS.AVAILABLE }] }),
    [keys.POINTINDEX_KEY(CC)]: JSON.stringify({ pA: 'A' }),
    [keys.LASTFEED_KEY(CC)]: JSON.stringify({ pA: CHARGER_STATUS.AVAILABLE }),
    [keys.META_KEY(CC)]: JSON.stringify({ lastHistoryHour: new Date().toISOString().slice(0, 13) }),
  }));
  const calls = { smembers: 0, scard: 0 };
  const redis = {
    async smembers(k) { calls.smembers++; return store.get(k) || []; },
    async scard(k) { calls.scard++; return (store.get(k) || []).length; },
    async mget(...ks) { return ks.map((k) => store.get(k) ?? null); },
    async mset(o) { for (const [k, v] of Object.entries(o)) store.set(k, v); },
    async get(k) { return store.get(k) ?? null; },
    async set(k, v) { store.set(k, v); },
  };
  const provider = {
    name: 'mobie', hasStatusFeed: true,
    async fetch() { return { statusStream: null }; },
    async parseStatus() { return new Map([['pA', { status: CHARGER_STATUS.CHARGING }]]); },
  };
  const res = await runStatusOnly(provider, { country: 'PT', dryRun: false }, redis);
  assert.strictEqual(res.upserted, 1);
  assert.strictEqual(calls.scard, 1);
  assert.strictEqual(calls.smembers, 0, 'delta path must not download the id set');
  assert.strictEqual(JSON.parse(store.get(keys.META_KEY(CC))).count, 1);
});
