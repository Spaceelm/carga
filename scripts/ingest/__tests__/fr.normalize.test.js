/**
 * Unit tests for providers/fr.js (France — national IRVE open data).
 * Run with:  node --test scripts/ingest/__tests__/fr.normalize.test.js
 *
 * CSV fixtures are inline strings (the provider accepts a string or a stream) so
 * each quirk under test is visible next to its assertion. Every quirk encoded
 * here was measured on the live feeds — see the header comment in fr.js.
 */

const test = require('node:test');
const assert = require('node:assert/strict');

const fr = require('../providers/fr');
const { CHARGER_STATUS, CONNECTOR_STANDARD } = require('../schema');

const STATIC_HEADER = [
  'nom_amenageur', 'nom_operateur', 'nom_enseigne', 'id_station_itinerance', 'nom_station',
  'implantation_station', 'adresse_station', 'coordonneesXY', 'nbre_pdc', 'id_pdc_itinerance',
  'id_pdc_local', 'puissance_nominale', 'prise_type_ef', 'prise_type_2', 'prise_type_combo_ccs',
  'prise_type_chademo', 'gratuit', 'paiement_acte', 'paiement_cb', 'tarification',
  'condition_acces', 'reservation', 'horaires', 'cable_t2_attache', 'date_maj',
  'consolidated_longitude', 'consolidated_latitude',
].join(',');

/** Quote a value the way the real feed does (commas/newlines/quotes inside fields). */
function csvEscape(v) {
  const s = String(v == null ? '' : v);
  return /[",\n\r]/.test(s) ? `"${s.replace(/"/g, '""')}"` : s;
}

/** Build a static CSV row from overrides (defaults = a plausible T2 point). */
function row(o = {}) {
  const d = {
    nom_amenageur: 'Amenageur', nom_operateur: 'OpCo', nom_enseigne: 'Enseigne',
    id_station_itinerance: 'FRABCP0001', nom_station: 'Station A',
    implantation_station: 'Parking public', adresse_station: '1 rue X 75001 Paris',
    coordonneesXY: '[2.35, 48.85]', nbre_pdc: '2', id_pdc_itinerance: 'FRABCE0001',
    id_pdc_local: 'L1', puissance_nominale: '22', prise_type_ef: 'false',
    prise_type_2: 'true', prise_type_combo_ccs: 'false', prise_type_chademo: 'false',
    gratuit: 'false', paiement_acte: 'true', paiement_cb: 'false', tarification: '',
    condition_acces: 'Accès libre', reservation: 'false', horaires: '24/7',
    cable_t2_attache: '', date_maj: '2026-07-30',
    consolidated_longitude: '2.35', consolidated_latitude: '48.85',
    ...o,
  };
  return STATIC_HEADER.split(',').map((k) => csvEscape(d[k])).join(',');
}

const staticCsv = (...rows) => [STATIC_HEADER, ...rows].join('\n') + '\n';

const DYN_HEADER = 'id_pdc_itinerance,etat_pdc,occupation_pdc,horodatage';
const iso = (msAgo) => new Date(Date.now() - msAgo).toISOString().replace('T', ' ').replace('Z', '+00:00');
const dynCsv = (...rows) => [DYN_HEADER, ...rows].join('\n') + '\n';

// ---------------------------------------------------------------------------
// CSV parser
// ---------------------------------------------------------------------------
test('CSV parser: quoted commas, embedded newlines, "" escapes, CRLF', async () => {
  const rows = [];
  const p = fr._makeCsvParser((r) => rows.push(r));
  p.write('a,b,c\r\n');
  p.write('"has, comma","line\nbreak","say ""hi"""\r\n');
  p.write('plain,,trailing\n');
  p.end();
  assert.deepEqual(rows[0], ['a', 'b', 'c']);
  assert.deepEqual(rows[1], ['has, comma', 'line\nbreak', 'say "hi"']);
  assert.deepEqual(rows[2], ['plain', '', 'trailing']);
});

test('CSV parser: survives input split mid-field across write() calls', async () => {
  const rows = [];
  const p = fr._makeCsvParser((r) => rows.push(r));
  p.write('a,b\n"spl');
  p.write('it, value",second\n');
  p.end();
  assert.deepEqual(rows[1], ['split, value', 'second']);
});

test('CSV parser: unterminated final quote does not hang or throw', async () => {
  const rows = [];
  const p = fr._makeCsvParser((r) => rows.push(r));
  p.write('a\n"truncated');
  p.end();
  assert.equal(rows.length, 2);
  assert.deepEqual(rows[1], ['truncated']);
});

// ---------------------------------------------------------------------------
// Field mappers
// ---------------------------------------------------------------------------
test('parseKw repairs watts published in the kW column', () => {
  assert.equal(fr._parseKw('22'), 22);
  assert.equal(fr._parseKw('22000'), 22);       // watts -> kW
  assert.equal(fr._parseKw('160000'), 160);     // watts -> kW
  assert.equal(fr._parseKw('7,4'), 7.4);        // comma decimal
  assert.equal(fr._parseKw('0'), null);
  assert.equal(fr._parseKw(''), null);
  assert.equal(fr._parseKw('abc'), null);
});

test('mapStandard prefers DC when a row claims several sockets', () => {
  assert.equal(fr._mapStandard({ prise_type_2: 'true', prise_type_combo_ccs: 'true' }), CONNECTOR_STANDARD.CCS);
  assert.equal(fr._mapStandard({ prise_type_2: 'true', prise_type_chademo: 'true' }), CONNECTOR_STANDARD.CHADEMO);
  assert.equal(fr._mapStandard({ prise_type_2: 'true' }), CONNECTOR_STANDARD.TYPE2);
  assert.equal(fr._mapStandard({ prise_type_ef: 'true' }), CONNECTOR_STANDARD.UNKNOWN);
});

test('mapFormat: DC always CABLE; T2 only when the feed states it', () => {
  assert.equal(fr._mapFormat({}, CONNECTOR_STANDARD.CCS), 'CABLE');
  assert.equal(fr._mapFormat({ cable_t2_attache: '' }, CONNECTOR_STANDARD.TYPE2), 'unknown');
  assert.equal(fr._mapFormat({ cable_t2_attache: 'true' }, CONNECTOR_STANDARD.TYPE2), 'CABLE');
  assert.equal(fr._mapFormat({ cable_t2_attache: 'false' }, CONNECTOR_STANDARD.TYPE2), 'SOCKET');
});

test('mapCapabilities emits NOTHING while paiement_cb is unknown', () => {
  // Critical: the client reads "non-empty capabilities without a card token" as
  // CONFIRMED no-card-payment and prunes the site from the Recommended filter, so
  // that array must never become non-empty off missing data. Every token counts —
  // including RESERVABLE, which says nothing about payment at all.
  assert.deepEqual([...fr._mapCapabilities({ paiement_cb: '', paiement_acte: 'true' })], []);
  assert.deepEqual([...fr._mapCapabilities({ paiement_cb: '', reservation: 'true' })], [],
    'RESERVABLE alone must not imply "no card payment"');
  assert.deepEqual([...fr._mapCapabilities({ paiement_cb: 'true' })], ['CREDIT_CARD_PAYABLE']);
  const no = [...fr._mapCapabilities({ paiement_cb: 'false', paiement_acte: 'true', reservation: 'true' })];
  assert.ok(!no.includes('CREDIT_CARD_PAYABLE'));
  assert.ok(no.includes('REMOTE_START_STOP_CAPABLE'), 'known-but-false cb still yields tokens');
  assert.ok(no.includes('RESERVABLE'));
});

test('open24h is tri-state: true / false / null for unknown hours', () => {
  // schema.js documents null = unknown; asserting false would tell the UI
  // "venue hours only" for a station that simply did not publish its hours.
  const one = async (horaires) => (await fr.normalize(staticCsv(row({ horaires })), null))[0].open24h;
  return Promise.all([
    one('24/7').then((v) => assert.equal(v, true)),
    one('Mo-Fr 08:00-18:00').then((v) => assert.equal(v, false)),
    one('').then((v) => assert.equal(v, null, 'blank hours must stay unknown')),
  ]);
});

test('connectors without a live observation carry no timestamp', () => {
  // Falling back to the producer's date_maj (seen back to 2020) would mix
  // years-old dates with fresh ISO ones in one station, and the client flips an
  // AVAILABLE plug to UNAVAILABLE when it looks >2 weeks staler than its siblings.
  return fr.normalize(staticCsv(row({ id_pdc_itinerance: 'FRABCE0001', date_maj: '2020-01-01' })), null)
    .then(([c]) => assert.equal(c.connectors[0].lastUpdated, null));
});

test('mapDynStatus: broken beats occupancy; only "libre" is available', () => {
  assert.equal(fr._mapDynStatus('hors_service', 'libre'), CHARGER_STATUS.OUT_OF_ORDER);
  assert.equal(fr._mapDynStatus('en_service', 'libre'), CHARGER_STATUS.AVAILABLE);
  assert.equal(fr._mapDynStatus('en_service', 'occupe'), CHARGER_STATUS.CHARGING);
  assert.equal(fr._mapDynStatus('en_service', 'reserve'), CHARGER_STATUS.CHARGING);
  assert.equal(fr._mapDynStatus('inconnu', 'inconnu'), CHARGER_STATUS.UNKNOWN);
});

test('parseHorodatage handles naive, offset and ISO forms', () => {
  assert.equal(fr._parseHorodatage('2026-07-31 10:00:00'), Date.parse('2026-07-31T10:00:00Z'));
  assert.equal(fr._parseHorodatage('2026-07-31 10:00:00.123456+00:00'), Date.parse('2026-07-31T10:00:00.123Z'));
  assert.equal(fr._parseHorodatage('2026-07-31T13:36:51+02:00'), Date.parse('2026-07-31T11:36:51Z'));
  assert.ok(Number.isNaN(fr._parseHorodatage('')));
});

test('parseCoords falls back to coordonneesXY and rejects junk', () => {
  assert.deepEqual(fr._parseCoords({ consolidated_latitude: '48.85', consolidated_longitude: '2.35' }), [48.85, 2.35]);
  assert.deepEqual(fr._parseCoords({ coordonneesXY: '[2.35, 48.85]' }), [48.85, 2.35]);
  assert.equal(fr._parseCoords({ consolidated_latitude: '0', consolidated_longitude: '0' }), null);
  assert.equal(fr._parseCoords({ consolidated_latitude: '999', consolidated_longitude: '2' }), null);
  assert.equal(fr._parseCoords({}), null);
});

// ---------------------------------------------------------------------------
// Status feed
// ---------------------------------------------------------------------------
test('parseStatus drops stale rows and keeps the freshest duplicate', async () => {
  const csv = dynCsv(
    `FRAAAE0001,en_service,libre,${iso(60 * 1000)}`,               // fresh
    `FRAAAE0002,en_service,libre,2020-12-29 14:39:41`,             // ancient -> dropped
    `FRAAAE0003,en_service,occupe,${iso(10 * 60 * 1000)}`,
    `FRAAAE0004,en_service,libre,${iso(80 * 3600 * 1000)}`,        // 80h old -> dropped
    `FRAAAE0005,en_service,occupe,${iso(30 * 60 * 1000)}`,         // older duplicate
    `FRAAAE0005,en_service,libre,${iso(60 * 1000)}`,               // fresher wins
    `bogus-id,en_service,libre,${iso(60 * 1000)}`,                 // bad id -> dropped
  );
  const m = await fr.parseStatus(csv);
  assert.equal(m.size, 3, 'FRAAAE0001, 0003 and the fresher 0005 survive');
  assert.equal(m.get('FRAAAE0001').status, CHARGER_STATUS.AVAILABLE);
  assert.ok(!m.has('FRAAAE0002'), 'ancient row dropped');
  assert.ok(!m.has('FRAAAE0004'), 'beyond staleness window dropped');
  assert.equal(m.get('FRAAAE0005').status, CHARGER_STATUS.AVAILABLE, 'freshest duplicate wins');
  assert.ok(!m.has('bogus-id'));
});

// ---------------------------------------------------------------------------
// Normalization
// ---------------------------------------------------------------------------
test('normalize groups charge points into one station', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001' }),
    row({ id_pdc_itinerance: 'FRABCE0002', puissance_nominale: '50000', prise_type_2: 'false', prise_type_combo_ccs: 'true' }),
  );
  const [c, ...rest] = await fr.normalize(csv, null);
  assert.equal(rest.length, 0, 'one station');
  assert.equal(c.id, 'FRABCP0001');
  assert.equal(c.country, 'FR');
  assert.equal(c.source, 'fr-irve');
  assert.equal(c.connectors.length, 2);
  assert.equal(c.open24h, true);
  assert.equal(c.parkingType, 'PARKING_LOT');
  assert.deepEqual(c.connectors.map((x) => x.powerKW), [22, 50]);
  assert.deepEqual(c.connectors.map((x) => x.standard), [CONNECTOR_STANDARD.TYPE2, CONNECTOR_STANDARD.CCS]);
  assert.equal(c.connectors[0].pointId, 'FRABCE0001');
  assert.equal(c.status, CHARGER_STATUS.UNKNOWN, 'no status feed -> unknown');
});

test('normalize joins live status by id_pdc_itinerance', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001' }),
    row({ id_pdc_itinerance: 'FRABCE0002' }),
  );
  const dyn = dynCsv(`FRABCE0001,en_service,libre,${iso(60 * 1000)}`);
  const [c] = await fr.normalize(csv, dyn);
  assert.equal(c.connectors[0].status, CHARGER_STATUS.AVAILABLE);
  assert.equal(c.connectors[1].status, CHARGER_STATUS.UNKNOWN, 'unlisted point stays unknown');
  assert.equal(c.status, CHARGER_STATUS.AVAILABLE, 'site aggregates to available');
});

test('normalize drops rows with unusable coordinates, keeps the rest', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001' }),
    row({ id_pdc_itinerance: 'FRABCE0009', consolidated_latitude: '', consolidated_longitude: '', coordonneesXY: '' }),
  );
  const out = await fr.normalize(csv, null);
  assert.equal(out.length, 1);
  assert.equal(out[0].connectors.length, 1);
});

test('normalize de-duplicates repeated pdc ids', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001' }),
    row({ id_pdc_itinerance: 'FRABCE0001' }),
  );
  const [c] = await fr.normalize(csv, null);
  assert.equal(c.connectors.length, 1);
});

test('normalize synthesizes a station id when the feed omits one', async () => {
  const csv = staticCsv(
    row({ id_station_itinerance: 'Non concerné', id_pdc_itinerance: 'FRABCE0001' }),
    row({ id_station_itinerance: '', id_pdc_itinerance: 'FRABCE0002' }),
  );
  const out = await fr.normalize(csv, null);
  assert.equal(out.length, 1, 'both rows share a coord-derived synthetic id');
  assert.ok(/^FRX/.test(out[0].id), `synthetic id, got ${out[0].id}`);
  assert.ok(!/[.\-\[\]]/.test(out[0].id), 'id is Redis/URL safe');
});

test('normalize emits a zero ad-hoc tariff only for gratuit points', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001', gratuit: 'true' }),
    row({ id_pdc_itinerance: 'FRABCE0002', gratuit: 'false' }),
  );
  const [c] = await fr.normalize(csv, null);
  assert.equal(c.connectors[0].tariffs[0].elements[0].price_components[0].price, 0);
  assert.equal(c.connectors[1].tariffs, undefined);
});

test('normalize tolerates quoted free text with commas and newlines', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001', tarification: '0,42 EUR/kWh, puis\n0,05 EUR/min' }),
  );
  const out = await fr.normalize(csv, null);
  assert.equal(out.length, 1);
  assert.equal(out[0].connectors.length, 1);
  assert.equal(out[0].connectors[0].powerKW, 22, 'columns after the quoted field stay aligned');
});

test('station open24h is true if ANY of its points is 24/7', async () => {
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001', horaires: 'Mo-Fr 08:00-18:00' }),
    row({ id_pdc_itinerance: 'FRABCE0002', horaires: '24/7' }),
  );
  const [c] = await fr.normalize(csv, null);
  assert.equal(c.open24h, true);
});

test('every FR evseId starts with FR, even without an itinerance id', async () => {
  // The client derives a charger's country from this id prefix to scope community
  // tariffs; an id that does not identify its country falls through to being priced
  // with ANOTHER country's tariffs. id_pdc_local is arbitrary producer text, so it
  // must never be used bare.
  const csv = staticCsv(
    row({ id_pdc_itinerance: 'FRABCE0001', id_pdc_local: 'BORNE-7' }),
    row({ id_pdc_itinerance: 'Non concerné', id_pdc_local: 'BORNE-8' }),
    row({ id_station_itinerance: 'Non concerné', id_pdc_itinerance: '', id_pdc_local: '42',
          consolidated_latitude: '45.75', consolidated_longitude: '4.85' }),
  );
  const out = await fr.normalize(csv, null);
  const ids = out.flatMap((c) => c.connectors.map((k) => k.evseId));
  assert.ok(ids.length >= 3, `expected all points to survive, got ${ids.length}`);
  for (const id of ids) {
    assert.match(id, /^FR/, `evseId "${id}" must be FR-prefixed`);
  }
  for (const c of out) assert.match(c.id, /^FR/, `station id "${c.id}" must be FR-prefixed`);
});

test('parseTariff keeps the AC and DC rates separate', () => {
  // The point of parsing per kind: narrowing the map to DC must price the DC plug
  // with the DC rate, not the site's cheapest.
  const p = fr._parseTariff('AC 0.36€/kWh - DC 0.59€/kWh');
  assert.equal(fr._rateForConnector(p, false).energy, 0.36);
  assert.equal(fr._rateForConnector(p, true).energy, 0.59);
  // HPC is French operator copy for high-power DC.
  const hpc = fr._parseTariff('HPC 49cts/Kwh');
  assert.equal(fr._rateForConnector(hpc, true).energy, 0.49);
  assert.equal(fr._rateForConnector(hpc, false), null, 'no AC rate quoted -> none');
});

test('parseTariff reads the common hand-written and centime forms', () => {
  const e = (s, dc) => {
    const r = fr._rateForConnector(fr._parseTariff(s), !!dc);
    return r ? r.energy : null;
  };
  assert.equal(e('0,29€ / kWh'), 0.29);      // decimal comma
  assert.equal(e('AC 36cts/KWh'), 0.36);     // centimes, must not parse as 6
  assert.equal(e('59 cts/kWh'), 0.59);
});

test('parseTariff keeps time and session fees instead of discarding the price', () => {
  // These are amortized by the client over its reference session, so they must
  // survive parsing rather than causing the price to be suppressed.
  const perMin = fr._rateForConnector(fr._parseTariff('0.27€/kWh+0.10€/min pour les non abonnées'), false);
  assert.equal(perMin.energy, 0.27);
  assert.equal(perMin.perMinute, 0.1);

  const sess = fr._rateForConnector(fr._parseTariff('Bornes rapides: 2€ + 0.59€ / kWh'), true);
  assert.equal(sess.energy, 0.59);
  assert.equal(sess.sessionFee, 2, 'connection fee must not be read as the energy rate');
});

test('parseTariff treats an overstay penalty as idle, not as charging cost', () => {
  const r = fr._rateForConnector(
    fr._parseTariff("entre 08:00 et 20:00 : 0.39€ par kwh de charge, 3.2€ par heure d'occupation hors charge"),
    false,
  );
  assert.equal(r.energy, 0.39);
  assert.equal(r.idlePerHour, 3.2);
  assert.equal(r.perMinute, null, 'an occupation fee must not inflate the cost of charging');
});

test('parseTariff always preserves the published wording', () => {
  // Even when nothing computable comes out, the text is the only pricing
  // information the charger has and is surfaced to the user.
  for (const s of ['Inconnu', 'https://x.fr/tarifs', 'Grille tarifaire en ligne', 'Payant']) {
    const p = fr._parseTariff(s);
    assert.equal(p.raw, s);
    assert.equal(p.rates.length, 0, `expected no rates for ${JSON.stringify(s)}`);
    assert.equal(fr._rateToTariff(fr._rateForConnector(p, false)), null);
  }
  assert.equal(fr._parseTariff(''), null);
  assert.equal(fr._parseTariff(null), null);
});

test('parseTariff rejects implausible rates and stray kWh mentions', () => {
  const e = (s) => { const r = fr._rateForConnector(fr._parseTariff(s), false); return r ? r.energy : null; };
  assert.equal(e('Recharge de 50 kWh disponible'), null);
  assert.equal(e('3500€/kWh'), null);
});

test('the connector carries the published wording verbatim', async () => {
  const txt = 'Tarification au kWh plus frais de connexion selon abonnement';
  const [c] = await fr.normalize(staticCsv(row({ tarification: txt })), null);
  assert.equal(c.connectors[0].tariffNote, txt);
  assert.equal(c.connectors[0].tariffs, undefined, 'nothing computable -> no tariff');
});

test('gratuit overrides the tarification prose', async () => {
  const [c] = await fr.normalize(staticCsv(row({ gratuit: 'true', tarification: '0,45€ / kWh' })), null);
  assert.equal(c.connectors[0].tariffs[0].elements[0].price_components[0].price, 0);
});

test('provider declares a 3-hour history window and a status feed', () => {
  assert.equal(fr.hasStatusFeed, true);
  assert.equal(fr.historyEveryHours, 3);
  assert.equal(fr.country, 'FR');
});
