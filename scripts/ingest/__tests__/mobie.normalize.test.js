/**
 * Unit test for the MOBI.E DATEX II normalizer.
 * Run with:  node --test scripts/ingest/__tests__/mobie.normalize.test.js
 *
 * Asserts the common-schema output against a trimmed real XML fixture:
 *   - connector type mapping (iec62196T2 -> Type 2, COMBO -> CCS, chademo -> CHAdeMO)
 *   - maxPowerAtSocket W -> kW conversion
 *   - live status join by refill-point id
 *   - coordinate / address / operator extraction
 *   - drop of sites missing coordinates
 */

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const mobie = require('../providers/mobie');
const { CHARGER_STATUS, CONNECTOR_STANDARD } = require('../schema');

const FIXTURES = path.join(__dirname, '..', '__fixtures__');
const staticXml = fs.readFileSync(path.join(FIXTURES, 'mobie-static.sample.xml'), 'utf8');
const statusXml = fs.readFileSync(path.join(FIXTURES, 'mobie-status.sample.xml'), 'utf8');

test('normalize() produces common-schema chargers with status join', async () => {
  const chargers = await mobie.normalize(staticXml, statusXml);

  // 3 sites in fixture, one has no coordinates -> dropped -> 2 usable.
  assert.strictEqual(chargers.length, 2, 'expected 2 usable chargers (no-coords site dropped)');

  const byId = Object.fromEntries(chargers.map((c) => [c.id, c]));

  // ---- Site 1: single AC Type 2 point, live "available" ----
  const amd = byId['EZC-AMD-00051'];
  assert.ok(amd, 'AMD site present');
  assert.strictEqual(amd.country, 'PT');
  assert.strictEqual(amd.source, 'mobie');
  assert.strictEqual(amd.name, 'AMD-00051');
  assert.strictEqual(amd.operator, 'EZ - CHARG3, Lda');
  assert.strictEqual(amd.city, 'Amadora');
  assert.strictEqual(amd.postcode, '2650-114');
  assert.strictEqual(amd.address, 'Estrada Santo Eloi, 25');
  assert.strictEqual(amd.lat, 38.78791);
  assert.strictEqual(amd.lon, -9.226767);
  assert.strictEqual(amd.status, CHARGER_STATUS.AVAILABLE);
  assert.strictEqual(amd.connectors.length, 1);

  const c0 = amd.connectors[0];
  assert.strictEqual(c0.standard, CONNECTOR_STANDARD.TYPE2, 'iec62196T2 -> Type 2');
  assert.strictEqual(c0.format, 'socket');
  assert.strictEqual(c0.chargingMode, 'mode3AC3p', 'charging mode parsed');
  // Auth methods -> capability tokens: creditcard + rfid + apps all mapped, so
  // "has auth data" is never mistaken for "no data".
  assert.deepStrictEqual(
    [...amd.capabilities].sort(),
    ['CREDIT_CARD_PAYABLE', 'REMOTE_START_STOP_CAPABLE', 'RFID_READER'],
    'auth methods -> capabilities'
  );
  assert.strictEqual(c0.powerKW, 11, '11000 W -> 11 kW');
  assert.strictEqual(c0.voltage, 400);
  assert.strictEqual(c0.amperage, 16);
  assert.strictEqual(c0.pointId, 'AMD-00051-1');
  assert.strictEqual(c0.evseId, 'PT*EZC*E*AMD*00051*1');
  assert.strictEqual(c0.status, CHARGER_STATUS.AVAILABLE, 'live status joined by point id');

  // ---- Site 2: two DC points (CCS charging, CHAdeMO outOfOrder) ----
  const gmr = byId['SGM-GMR-00022'];
  assert.ok(gmr, 'GMR site present');
  assert.strictEqual(gmr.connectors.length, 2);
  assert.strictEqual(gmr.operator, 'São Gonçalo Mobilidade');

  const ccs = gmr.connectors.find((c) => c.pointId === 'GMR-00022-01');
  assert.strictEqual(ccs.standard, CONNECTOR_STANDARD.CCS, 'iec62196T2COMBO -> CCS');
  assert.strictEqual(ccs.powerKW, 150, '150000 W -> 150 kW');
  assert.strictEqual(ccs.status, CHARGER_STATUS.CHARGING);
  assert.strictEqual(ccs.chargingMode, 'mode4DC', 'DC charging mode parsed');
  assert.deepStrictEqual(gmr.capabilities, [], 'no auth methods -> no capabilities');

  const chademo = gmr.connectors.find((c) => c.pointId === 'GMR-00022-02');
  assert.strictEqual(chademo.standard, CONNECTOR_STANDARD.CHADEMO, 'chademo -> CHAdeMO');
  assert.strictEqual(chademo.powerKW, 50, '50000 W -> 50 kW');
  assert.strictEqual(chademo.status, CHARGER_STATUS.OUT_OF_ORDER);

  // Aggregate site status: at least one charging, none available -> charging.
  assert.strictEqual(gmr.status, CHARGER_STATUS.CHARGING);
});

test('normalize() without status feed defaults connectors to unknown', async () => {
  const chargers = await mobie.normalize(staticXml, null);
  const amd = chargers.find((c) => c.id === 'EZC-AMD-00051');
  assert.strictEqual(amd.connectors[0].status, CHARGER_STATUS.UNKNOWN);
});

test('unit helpers: connector mapping, W->kW, status mapping', () => {
  assert.strictEqual(mobie._mapConnectorType('iec62196T2'), CONNECTOR_STANDARD.TYPE2);
  assert.strictEqual(mobie._mapConnectorType('iec62196T2COMBO'), CONNECTOR_STANDARD.CCS);
  assert.strictEqual(mobie._mapConnectorType('chademo'), CONNECTOR_STANDARD.CHADEMO);
  assert.strictEqual(mobie._mapConnectorType('weird'), CONNECTOR_STANDARD.UNKNOWN);

  assert.strictEqual(mobie._wattsToKW('22000.0'), 22);
  assert.strictEqual(mobie._wattsToKW('0'), null);
  assert.strictEqual(mobie._wattsToKW('abc'), null);

  assert.strictEqual(mobie._mapStatus('available'), CHARGER_STATUS.AVAILABLE);
  assert.strictEqual(mobie._mapStatus('inoperative'), CHARGER_STATUS.OUT_OF_ORDER);
  assert.strictEqual(mobie._mapStatus('blocked'), CHARGER_STATUS.OUT_OF_ORDER);
  assert.strictEqual(mobie._mapStatus('planned'), CHARGER_STATUS.PLANNED);
});

test('parseStatus() maps ids to statuses', async () => {
  const map = await mobie.parseStatus(statusXml);
  assert.strictEqual(map.size, 3);
  assert.strictEqual(map.get('AMD-00051-1').status, CHARGER_STATUS.AVAILABLE);
  assert.strictEqual(map.get('GMR-00022-01').status, CHARGER_STATUS.CHARGING);
  assert.strictEqual(map.get('GMR-00022-02').status, CHARGER_STATUS.OUT_OF_ORDER);
});
