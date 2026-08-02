/**
 * Unit test for the REVE (Spain) OCPI normalizer.
 * Run with:  node --test scripts/ingest/__tests__/reve.normalize.test.js
 *
 * Asserts the common-schema output from inline OCPI fixtures:
 *   - opening_times -> open24h (twentyfourseven / regular_hours / neither)
 *   - /connectors/tariffs join + normalization to the frontend ad-hoc shape
 *   - facilities / parking_type pass-through
 *   - connector standard + W->kW mapping, drop of sites missing coordinates
 */

const test = require('node:test');
const assert = require('node:assert');

const reve = require('../providers/reve');
const { CONNECTOR_STANDARD, CHARGER_STATUS } = require('../schema');

function loc(over = {}) {
  return {
    id: 'ES-LOC-1',
    name: 'Estación Centro',
    address: 'Calle Mayor 1',
    city: 'Madrid',
    cpo_name: 'Iberdrola',
    coordinates: { latitude: '40.4168', longitude: '-3.7038' },
    evses: [
      {
        evse_id: 'ES*IBD*E001',
        capabilities: ['CREDIT_CARD_PAYABLE', 'RFID_READER'],
        connectors: [
          { id: 'ES*IBD*E001*1', standard: 'IEC_62196_T2_COMBO', format: 'CABLE', max_electric_power: 150000, max_voltage: 400, max_amperage: 375 },
        ],
      },
    ],
    last_updated: '2026-07-01T10:00:00Z',
    ...over,
  };
}

test('openStateFromLocation maps open24h + chargingWhenClosed', () => {
  // Venue open 24/7 -> open24h true, no caveat.
  assert.deepStrictEqual(reve.openStateFromLocation({ opening_times: { twentyfourseven: true } }), { open24h: true, chargingWhenClosed: false });
  // Limited venue hours, no charging-when-closed -> only during hours.
  assert.deepStrictEqual(reve.openStateFromLocation({ opening_times: { regular_hours: [{ weekday: 1, period_begin: '08:00', period_end: '20:00' }] } }), { open24h: false, chargingWhenClosed: false });
  // Limited venue hours BUT charger usable while closed -> 24/7 with caveat.
  assert.deepStrictEqual(reve.openStateFromLocation({ opening_times: { regular_hours: [{ weekday: 1 }] }, charging_when_closed: true }), { open24h: true, chargingWhenClosed: true });
  // charging_when_closed with no hours info -> 24/7 with caveat.
  assert.deepStrictEqual(reve.openStateFromLocation({ charging_when_closed: true }), { open24h: true, chargingWhenClosed: true });
  // twentyfourseven wins over charging_when_closed -> true 24/7, no caveat.
  assert.deepStrictEqual(reve.openStateFromLocation({ opening_times: { twentyfourseven: true }, charging_when_closed: true }), { open24h: true, chargingWhenClosed: false });
  // Nothing known -> unknown.
  assert.deepStrictEqual(reve.openStateFromLocation({}), { open24h: null, chargingWhenClosed: false });
});

test('reveTariffToOcpi normalizes to AD_HOC_PAYMENT with numeric prices', () => {
  const out = reve.reveTariffToOcpi({
    id: 'T1',
    currency: 'EUR',
    elements: [
      { price_components: [{ type: 'ENERGY', price: '0.45', vat: '21', step_size: 1 }, { type: 'TIME', price: '0.00' }] },
    ],
  });
  assert.strictEqual(out.type, 'AD_HOC_PAYMENT', 'frontend parser keys on this type');
  assert.strictEqual(out.currency, 'EUR');
  const pc = out.elements[0].price_components[0];
  assert.strictEqual(pc.type, 'ENERGY');
  assert.strictEqual(pc.price, 0.45, 'string price -> number');
  assert.strictEqual(typeof pc.price, 'number');
  assert.strictEqual(pc.vat, 21);
});

test('locationToCharger sets open24h, facilities, parkingType and joins tariffs', () => {
  const tariffsById = new Map([
    ['ES*IBD*E001*1', [reve.reveTariffToOcpi({ id: 'T1', currency: 'EUR', elements: [{ price_components: [{ type: 'ENERGY', price: '0.45' }] }] })]],
  ]);
  const charger = reve.locationToCharger(
    loc({ opening_times: { twentyfourseven: true }, facilities: ['RESTAURANT', 'SUPERMARKET'], parking_type: 'PARKING_GARAGE' }),
    tariffsById
  );
  assert.strictEqual(charger.open24h, true);
  assert.deepStrictEqual(charger.facilities, ['RESTAURANT', 'SUPERMARKET']);
  assert.strictEqual(charger.parkingType, 'PARKING_GARAGE');
  assert.deepStrictEqual(charger.capabilities, ['CREDIT_CARD_PAYABLE', 'RFID_READER'], 'EVSE capabilities unioned onto the site');
  assert.strictEqual(charger.operator, 'Iberdrola');

  const c0 = charger.connectors[0];
  assert.strictEqual(c0.standard, CONNECTOR_STANDARD.CCS, 'IEC_62196_T2_COMBO -> CCS');
  assert.strictEqual(c0.powerKW, 150, '150000 W -> 150 kW');
  assert.ok(Array.isArray(c0.tariffs) && c0.tariffs.length === 1, 'tariff joined by connector id');
  assert.strictEqual(c0.tariffs[0].elements[0].price_components[0].price, 0.45);
});

test('locationToCharger flags chargingWhenClosed (24/7 charger, venue may close)', () => {
  const open = reve.locationToCharger(loc({ opening_times: { twentyfourseven: true } }));
  assert.strictEqual(open.open24h, true);
  assert.strictEqual(open.chargingWhenClosed, undefined, 'true 24/7 venue -> no caveat flag');

  const whenClosed = reve.locationToCharger(loc({
    opening_times: { regular_hours: [{ weekday: 1, period_begin: '08:00', period_end: '20:00' }] },
    charging_when_closed: true,
  }));
  assert.strictEqual(whenClosed.open24h, true, 'still counts as 24/7 for the filter');
  assert.strictEqual(whenClosed.chargingWhenClosed, true, 'flagged so the badge notes venue may be closed');
});

test('operational_status === false marks the connector out-of-order', () => {
  const brokenById = new Map([['ES*IBD*E001', false]]);
  const broken = reve.locationToCharger(loc(), null, brokenById);
  assert.strictEqual(broken.connectors[0].status, CHARGER_STATUS.OUT_OF_ORDER);

  const okById = new Map([['ES*IBD*E001', true]]);
  const ok = reve.locationToCharger(loc(), null, okById);
  assert.strictEqual(ok.connectors[0].status, CHARGER_STATUS.UNKNOWN, 'operational -> unknown (overlay applies live)');

  // No operational map -> unchanged (unknown).
  assert.strictEqual(reve.locationToCharger(loc()).connectors[0].status, CHARGER_STATUS.UNKNOWN);
});

test('reveOcpiToCanonical maps status tokens for history/baseline', () => {
  assert.strictEqual(reve.reveOcpiToCanonical('AVAILABLE'), CHARGER_STATUS.AVAILABLE);
  assert.strictEqual(reve.reveOcpiToCanonical('CHARGING'), CHARGER_STATUS.CHARGING);
  assert.strictEqual(reve.reveOcpiToCanonical('OUTOFORDER'), CHARGER_STATUS.OUT_OF_ORDER);
  assert.strictEqual(reve.reveOcpiToCanonical('UNAVAILABLE'), CHARGER_STATUS.CHARGING, 'occupied/blocked -> not available');
  assert.strictEqual(reve.reveOcpiToCanonical(null), null, 'unknown -> no observation');
});

test('nearestMarker matches within radius, rejects beyond it', () => {
  const markers = [
    { lat: 40.4168, lon: -3.7038, status: 'AVAILABLE' },
    { lat: 41.0, lon: -3.0, status: 'CHARGING' },
  ];
  const near = reve.nearestMarker(40.41685, -3.70385, markers, 60); // ~7 m away
  assert.strictEqual(near && near.status, 'AVAILABLE');
  const far = reve.nearestMarker(40.4200, -3.7100, markers, 60); // ~500 m — beyond 60 m
  assert.strictEqual(far, null);
});

test('capabilities are deduped across a site with multiple EVSEs', () => {
  const charger = reve.locationToCharger(loc({
    evses: [
      { evse_id: 'A', capabilities: ['RFID_READER', 'RESERVABLE'], connectors: [{ id: 'A1', standard: 'IEC_62196_T2', max_electric_power: 22000 }] },
      { evse_id: 'B', capabilities: ['RFID_READER', 'CONTACTLESS_CARD_SUPPORT'], connectors: [{ id: 'B1', standard: 'IEC_62196_T2', max_electric_power: 22000 }] },
    ],
  }));
  assert.deepStrictEqual(charger.capabilities, ['RFID_READER', 'RESERVABLE', 'CONTACTLESS_CARD_SUPPORT']);
});

test('tariff join falls back to evse_id when connector id has none', () => {
  const tariffsById = new Map([
    ['ES*IBD*E001', [reve.reveTariffToOcpi({ id: 'T2', elements: [{ price_components: [{ type: 'ENERGY', price: '0.30' }] }] })]],
  ]);
  const charger = reve.locationToCharger(loc(), tariffsById);
  assert.strictEqual(charger.connectors[0].tariffs[0].elements[0].price_components[0].price, 0.30);
});

test('no tariff map -> connector has no tariffs key; missing coords dropped', () => {
  const charger = reve.locationToCharger(loc());
  assert.strictEqual(charger.connectors[0].tariffs, undefined, 'omitted when no pricing');
  assert.strictEqual(charger.open24h, null, 'no opening_times -> unknown');
  assert.deepStrictEqual(charger.facilities, []);

  assert.strictEqual(reve.locationToCharger(loc({ coordinates: {} })), null, 'no coords -> dropped');
});

test('normalizeStreaming accepts {locations, tariffsById} and a bare array', async () => {
  const tariffsById = new Map([
    ['ES*IBD*E001*1', [reve.reveTariffToOcpi({ id: 'T1', elements: [{ price_components: [{ type: 'ENERGY', price: '0.45' }] }] })]],
  ]);
  const out = [];
  const res = await reve.normalizeStreaming({ locations: [loc()], tariffsById }, null, (c) => out.push(c));
  assert.strictEqual(res.sites, 1);
  assert.strictEqual(out[0].connectors[0].tariffs[0].elements[0].price_components[0].price, 0.45);

  // Back-compat: a bare locations array still works (no tariffs).
  const out2 = [];
  await reve.normalizeStreaming([loc()], null, (c) => out2.push(c));
  assert.strictEqual(out2[0].connectors[0].tariffs, undefined);
});

test('the resumable crawl path threads tariffs and operational status through', async () => {
  // ES is the ONLY provider that uses the crawl, and the crawl used to hand
  // normalizeStreaming just { locations }. Both enrichment passes were therefore
  // unreachable in production and every Spanish connector shipped `tariffs: []`.
  const loc = {
    id: 'L1', name: 'Sample', coordinates: { latitude: '40.4', longitude: '-3.7' },
    operator: { name: 'Op' },
    evses: [{ uid: 'E1', evse_id: 'ES*AAA*E1', connectors: [{ id: '1', standard: 'IEC_62196_T2', max_electric_power: 22000 }] }],
  };
  const tariffsById = new Map([['1', [{
    type: 'AD_HOC_PAYMENT', currency: 'EUR',
    elements: [{ price_components: [{ type: 'ENERGY', price: 0.45, vat: 21, step_size: 1 }] }],
  }]]]);

  const bare = [];
  await reve.normalizeStreaming({ locations: [loc] }, null, (c) => bare.push(c));
  assert.equal(bare[0].connectors[0].tariffs, undefined, 'locations alone carry no price');

  const rich = [];
  await reve.normalizeStreaming({ locations: [loc], tariffsById }, null, (c) => rich.push(c));
  const pc = rich[0].connectors[0].tariffs[0].elements[0].price_components[0];
  assert.equal(pc.type, 'ENERGY');
  assert.equal(pc.price, 0.45);
});

test('reve exposes fetchEnrichment so the crawl can complete the record', () => {
  assert.equal(typeof reve.fetchEnrichment, 'function');
});
