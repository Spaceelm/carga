// MOBI.E per-plug operator fees (providers/mobie-tariffs.js).
// What these pin: the right plug gets the right fee; a time-windowed fee keeps its
// window; a broken download or a run that prices too few plugs FAILS the job instead
// of silently writing stations without fees (which is how fees vanished in 2026).
const test = require('node:test');
const assert = require('node:assert');
const T = require('../providers/mobie-tariffs');

const stations = {};
for (let i = 0; i < 3000; i++) stations[`ZZZ-${String(i).padStart(5, '0')}`] = { 1: 0 };
const FILE = {
  source: { system: 'pdgr', fetchedAt: '2026-11-14T00:02:25.915Z' },
  sets: [
    // 0: flat fee, energy and time all session
    [{ plan: 'regular', price: 0.261, unit: 'charge', after: null, until: null },
      { plan: 'regular', price: 0.1, unit: 'kWh', after: null, until: null },
      { plan: 'regular', price: 0.015, unit: 'min', after: null, until: null },
      { plan: 'adhoc', price: 0.9, unit: 'kWh', after: null, until: null }],
    // 1: energy for the first 60 min, then a per-minute fee
    [{ plan: 'regular', price: 0.2499, unit: 'charge', after: null, until: null },
      { plan: 'regular', price: 0.25, unit: 'kWh', after: null, until: 60 },
      { plan: 'regular', price: 0.2, unit: 'min', after: 60, until: null }],
    // 2: ad hoc only — a plan with no regular part is a known zero fee
    [{ plan: 'adhoc', price: 0.5285, unit: 'kWh', after: null, until: null }],
  ],
  stations: { ...stations, 'LSB-00538': { 1: 0, 2: 1 }, 'VNG-00173': { 539: 0, 540: 1, 541: 2 }, 'ABF-00008': { 1: 2 } },
};

test('plugKey reads every EVSE id style the feed uses, then the point id', () => {
  assert.deepStrictEqual(T.plugKey('PT*GLP*ELSB*00538*01'), { station: 'LSB-00538', plug: '1' });
  assert.deepStrictEqual(T.plugKey('PT*EPK*ELSB00736*1'), { station: 'LSB-00736', plug: '1' });
  assert.deepStrictEqual(T.plugKey('PT*FCT*E*VNG*00173*02'), { station: 'VNG-00173', plug: '2' });
  assert.deepStrictEqual(T.plugKey(null, 'LSB-00538-02'), { station: 'LSB-00538', plug: '2' });
  assert.strictEqual(T.plugKey('PT*EPK*ELACF003*1', '0952893D-06D8'), null, 'not a MOBI.E station id: no guess');
});

test('the right plug gets the right fee, windows kept, ad hoc ignored', () => {
  const idx = T.parseTariffs(FILE);
  assert.deepStrictEqual(T.opcForPlug(idx, { evseId: 'PT*GLP*ELSB*00538*01' }), { energy: 0.1, time: 0.015, flat: 0.261 });
  assert.deepStrictEqual(T.opcForPlug(idx, { evseId: 'PT*GLP*ELSB*00538*02' }), {
    energy: 0, time: 0, flat: 0.2499,
    parts: [{ unit: 'charge', price: 0.2499 }, { unit: 'kWh', price: 0.25, until: 60 }, { unit: 'min', price: 0.2, after: 60 }],
  });
  assert.deepStrictEqual(T.opcForPlug(idx, { evseId: 'PT*EDP*EABF*00008*01' }), { energy: 0, time: 0, flat: 0 }, 'no regular part: known zero');
  assert.strictEqual(T.opcForPlug(idx, { evseId: 'PT*GLP*ELSB*00538*03' }), null, 'a plug the station does not list');
  assert.strictEqual(T.opcForPlug(idx, { evseId: 'PT*GLP*ELSB*99999*01' }), null, 'a station not in the file');
});

test('globally numbered plugs map by position; ordinal ones never do', () => {
  const idx = T.parseTariffs(FILE);
  assert.strictEqual(T.opcForPlug(idx, { evseId: 'PT*FCT*E*VNG*00173*01' }).flat, 0.261);
  assert.strictEqual(T.opcForPlug(idx, { evseId: 'PT*FCT*E*VNG*00173*02' }).flat, 0.2499);
  assert.strictEqual(T.setIndexFor({ 1: 0, 3: 1 }, '2'), null, 'ordinal keys with a gap: plug 2 is missing, not plug 3');
});

test('a file that is not the fee list is refused', () => {
  assert.throws(() => T.parseTariffs('<html>informacao-institucional</html>'));
  assert.throws(() => T.parseTariffs({ sets: [], stations: { 'A-1': {} } }), /only 1 stations/);
});

test('a failed download fails the job but returns null so stations are still written', async () => {
  const errors = [];
  const orig = console.error;
  console.error = (m) => errors.push(m);
  const before = process.exitCode;
  try {
    const html = { ok: true, status: 200, headers: { get: () => 'text/html' }, text: async () => '<html></html>' };
    assert.strictEqual(await T.fetchTariffs(async () => html, 'ua'), null);
    assert.strictEqual(process.exitCode, 1);
    assert.match(errors[0], /^::error title=MOBI\.E fees::fee download failed \(expected JSON, got text\/html\)/);
  } finally {
    console.error = orig;
    process.exitCode = before;
  }
});

test('too few plugs priced fails the job; a normal run does not', () => {
  const orig = { log: console.log, error: console.error };
  const before = process.exitCode;
  console.log = () => {}; console.error = () => {};
  try {
    const low = T.coverageTracker();
    low.count([...Array(600)].map((_, i) => (i < 100 ? { opc: {} } : {})));
    assert.strictEqual(low.report({}).priced, 100);
    assert.strictEqual(process.exitCode, 1);
    process.exitCode = before;
    const ok = T.coverageTracker();
    ok.count([...Array(600)].map((_, i) => (i < 450 ? { opc: {} } : {})));
    ok.report({});
    assert.strictEqual(process.exitCode, before);
  } finally {
    Object.assign(console, orig);
    process.exitCode = before;
  }
});

// --- network membership --------------------------------------------------------------

const STATIONS = { stations: [
  ...[...Array(5000)].map((_, i) => ({ name: `ZZZ-${String(i).padStart(5, '0')}`, mobieNetwork: true, sockets: [] })),
  { name: 'LSB-00538', mobieNetwork: true, sockets: [{ evse: 'LSB-00538-01' }] },
  { name: 'A0104PL0001', operator: 'Continente', mobieNetwork: false, sockets: [{ evse: '254' }] },
  { name: null, mobieNetwork: false, sockets: [{ evse: 'PTM-OCEANO-01' }] },
  { name: 'X-NOFLAG', sockets: [] },
] };

test('network flag: from the plug, the point id, or the site id without its operator prefix', () => {
  const idx = T.parseStations(STATIONS);
  assert.strictEqual(T.networkForSite(idx, { id: 'GLP-LSB-00538' }, [{ evseId: 'PT*GLP*ELSB*00538*01' }]), true);
  assert.strictEqual(T.networkForSite(idx, { id: 'CNT-A0104PL0001' }, [{ evseId: 'PT*CNT*EA0104PL0001*1' }]), false, 'Continente: outside MOBI.E');
  assert.strictEqual(T.networkForSite(idx, { id: 'HGP-X' }, [{ evseId: 'PT*HGP*E184', pointId: 'PTM-OCEANO-01' }]), false);
  assert.strictEqual(T.networkForSite(idx, { id: 'EDP-ALM-99999' }, [{ evseId: 'PT*EDP*EALM*99999*1' }]), null, 'not listed: unknown, never a guess');
  assert.strictEqual(T.networkForSite(idx, { id: 'ABC-X-NOFLAG' }, []), null, 'listed without a flag: unknown');
  assert.strictEqual(T.networkForSite(null, { id: 'GLP-LSB-00538' }, []), null);
});

test('a station list that is not the list is refused', () => {
  assert.throws(() => T.parseStations('<html></html>'));
  assert.throws(() => T.parseStations({ stations: [{ name: 'A', mobieNetwork: true }] }), /lists 1 stations/);
});
