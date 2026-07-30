/**
 * Unit test for the DGT (Spain) DATEX II normalizer.
 * Run with:  node --test scripts/ingest/__tests__/dgt.normalize.test.js
 *
 * Asserts the common-schema output against a trimmed real XML fixture
 * (sites sliced from infocar.dgt.es electrolineras.xml):
 *   - coordinates read from `coordinatesForDisplay` (NOT `pointCoordinates`)
 *   - connector type mapping (iec62196T2 -> Type 2, COMBO -> CCS, chademo -> CHAdeMO)
 *   - maxPowerAtSocket W -> kW conversion
 *   - stable id scheme: "ES-" + energyInfrastructureSite@id (no PT collision)
 *   - status defaults to "unknown" (Spain has no live-status feed)
 *   - a refillPoint with MULTIPLE connectors emits one connector each
 *   - labeled address lines -> address ("Dirección:") + city ("Municipio:")
 *   - drop of sites missing coordinates
 */

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const path = require('path');

const dgt = require('../providers/dgt');
const { CHARGER_STATUS, CONNECTOR_STANDARD } = require('../schema');

const FIXTURES = path.join(__dirname, '..', '__fixtures__');
const staticXml = fs.readFileSync(path.join(FIXTURES, 'dgt.sample.xml'), 'utf8');

test('normalize() produces common-schema chargers (static-only, status=unknown)', async () => {
  const chargers = await dgt.normalize(staticXml);

  // 3 sites in fixture, one has no coordinates -> dropped -> 2 usable.
  assert.strictEqual(chargers.length, 2, 'expected 2 usable chargers (no-coords site dropped)');

  const byId = Object.fromEntries(chargers.map((c) => [c.id, c]));

  // ---- Site 1: AC site, two Type 2 refill points ----
  const s1 = byId['ES-HQXAC9HN2SYEETBCU5SO'];
  assert.ok(s1, 'AC site present with ES- prefixed id');
  assert.strictEqual(s1.country, 'ES');
  assert.strictEqual(s1.source, 'dgt');
  assert.strictEqual(s1.name, 'Malecón de la Encantá 3');
  assert.strictEqual(s1.operator, 'QWELLO España SL');
  assert.strictEqual(s1.postcode, '3170');
  assert.strictEqual(s1.address, 'Malecón de la Encantá 3', 'street from "Dirección:" line');
  assert.strictEqual(s1.city, 'Rojales', 'city from "Municipio:" line');
  assert.strictEqual(s1.lat, 38.087696, 'lat from coordinatesForDisplay');
  assert.strictEqual(s1.lon, -0.72432035, 'lon from coordinatesForDisplay');
  assert.strictEqual(s1.status, CHARGER_STATUS.UNKNOWN, 'no ES status feed -> unknown');
  assert.strictEqual(s1.connectors.length, 2);

  const c0 = s1.connectors[0];
  assert.strictEqual(c0.standard, CONNECTOR_STANDARD.TYPE2, 'iec62196T2 -> Type 2');
  assert.strictEqual(c0.format, 'socket');
  assert.strictEqual(c0.powerKW, 22, '22000 W -> 22 kW');
  assert.strictEqual(c0.voltage, null, 'no voltage element in feed -> null');
  assert.strictEqual(c0.amperage, 32);
  assert.strictEqual(c0.pointId, 'WDXF5ZZ7YDDT0BUEGOQ15JFXLSM', 'stable refillPoint@id');
  assert.strictEqual(c0.evseId, 'ES*AEQ*ESAEQE5PFX311', 'evseId from refillPoint name');
  assert.strictEqual(c0.status, CHARGER_STATUS.UNKNOWN);

  // ---- Site 2: one refillPoint exposing BOTH CCS and CHAdeMO ----
  const s2 = byId['ES-2025000364'];
  assert.ok(s2, 'DC site present');
  assert.strictEqual(s2.operator, 'CENTROS COMERCIALES CARREFOUR SA');
  assert.strictEqual(s2.city, 'Badajoz');
  assert.strictEqual(
    s2.connectors.length,
    2,
    'one refillPoint with two <connector> children -> two schema connectors'
  );

  const ccs = s2.connectors.find((c) => c.standard === CONNECTOR_STANDARD.CCS);
  assert.ok(ccs, 'iec62196T2COMBO -> CCS');
  assert.strictEqual(ccs.powerKW, 50, '50000 W -> 50 kW');
  assert.strictEqual(ccs.status, CHARGER_STATUS.UNKNOWN);

  const chademo = s2.connectors.find((c) => c.standard === CONNECTOR_STANDARD.CHADEMO);
  assert.ok(chademo, 'chademo -> CHAdeMO');
  assert.strictEqual(chademo.powerKW, 50, '50000 W -> 50 kW');

  // Multi-connector refillPoint: point ids suffixed so each connector is unique.
  assert.strictEqual(ccs.pointId, 'COD20250000049157PR00001480-1');
  assert.strictEqual(chademo.pointId, 'COD20250000049157PR00001480-2');
});

test('normalize() drops sites without coordinates', async () => {
  const chargers = await dgt.normalize(staticXml);
  assert.ok(!chargers.some((c) => c.id === 'ES-ESNOCOORDS0001'), 'no-coords site dropped');
});

test('unit helpers: connector mapping and W->kW', () => {
  assert.strictEqual(dgt._mapConnectorType('iec62196T2'), CONNECTOR_STANDARD.TYPE2);
  assert.strictEqual(dgt._mapConnectorType('iec62196T2COMBO'), CONNECTOR_STANDARD.CCS);
  assert.strictEqual(dgt._mapConnectorType('chademo'), CONNECTOR_STANDARD.CHADEMO);
  assert.strictEqual(dgt._mapConnectorType('domesticF'), CONNECTOR_STANDARD.UNKNOWN);

  assert.strictEqual(dgt._wattsToKW('50000.0'), 50);
  assert.strictEqual(dgt._wattsToKW('0'), null);
  assert.strictEqual(dgt._wattsToKW('abc'), null);
});

test('parseStatus() is an empty map (Spain has no status feed)', async () => {
  const map = await dgt.parseStatus();
  assert.strictEqual(map.size, 0);
});
