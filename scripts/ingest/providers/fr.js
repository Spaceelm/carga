/**
 * providers/fr.js
 *
 * France — national IRVE open data (transport.data.gouv.fr, Licence Ouverte 2.0,
 * no API key). France mandates publication by decree: ~279 separate producers
 * (CPOs, Gireve, eco-movement, QualiCharge) publish to data.gouv.fr and the
 * National Access Point consolidates them. We read two files:
 *
 *   static  (resource 84013): "[BETA] Base Nationale des Points de Recharge" —
 *            the DEDUPLICATED consolidation (~165k charge points / ~47k stations,
 *            ~120 MB CSV, refreshed daily). One row per charge point (pdc).
 *   status  (resource 84098): consolidated "IRVE dynamique" (~9 MB CSV) — per-pdc
 *            etat/occupation rows in the schema-irve-dynamique format, OCPI-fed by
 *            operators via Gireve/QualiCharge. Only operators publishing dynamic
 *            data appear (~124k pdc listed, freshness varies per producer), so a
 *            staleness gate below decides what we trust.
 *
 * Join key: id_pdc_itinerance (AFIREV id, e.g. "FRE10E85529") — used as
 * Connector.pointId, exactly like MOBI.E refill-point ids for PT.
 *
 * Data-quality quirks measured on the live feeds (2026-07-31) and handled here:
 *   - puissance_nominale is kW by schema, but ~900 rows carry WATTS (22000) —
 *     values > 1000 are divided by 1000.
 *   - cable_t2_attache is 100% empty in the BETA consolidation → T2 format stays
 *     "unknown" (DC connectors are physically tethered → "CABLE").
 *   - booleans are lowercase "true"/"false", sometimes empty (= unknown).
 *   - coordinates: consolidated_longitude/latitude are always parseable; the raw
 *     coordonneesXY "[lon, lat]" string is the fallback.
 *   - free-text fields (tarification, observations) contain commas AND newlines
 *     inside quotes → the CSV parser is a real quote-aware state machine.
 *   - dynamique horodatage is naive "YYYY-MM-DD HH:MM:SS" (treated as UTC; the
 *     ≤2h zone ambiguity is irrelevant against a 24h staleness gate).
 */

const { CHARGER_STATUS, CONNECTOR_STANDARD } = require('../schema');

const STATIC_URL = process.env.FR_STATIC_URL
  || 'https://transport.data.gouv.fr/resources/84013/download';
const STATUS_URL = process.env.FR_STATUS_URL
  || 'https://transport.data.gouv.fr/resources/84098/download';
const USER_AGENT = 'carga-live-web/1.0';
const COUNTRY = 'FR';
const SOURCE = 'fr-irve';

// Dynamic rows older than this are ignored (the pdc then reads "unknown" — grey
// on the map rather than a stale claim of availability). The consolidated file
// still carries rows last updated in 2020.
const STALE_HOURS = (() => {
  const v = parseFloat(process.env.FR_STATUS_STALE_HOURS || '');
  return isFinite(v) && v > 0 ? v : 24;
})();

// ---------------------------------------------------------------------------
// CSV: streaming, quote-aware (RFC 4180-ish) state machine. No dependencies.
// Handles: quoted fields containing commas/newlines/CRLF, "" escapes, UTF-8
// input split across chunk boundaries (callers decode with StringDecoder).
// ---------------------------------------------------------------------------
function makeCsvParser(onRow) {
  let field = '';
  let row = [];
  let inQuotes = false;
  let quotePending = false; // saw '"' inside quotes; the next char disambiguates

  const endField = () => { row.push(field); field = ''; };
  const endRow = () => {
    endField();
    if (row.length > 1 || row[0] !== '') onRow(row);
    row = [];
  };

  return {
    write(text) {
      for (let i = 0; i < text.length; i++) {
        const ch = text[i];
        if (quotePending) {
          quotePending = false;
          if (ch === '"') { field += '"'; continue; } // "" -> literal quote
          inQuotes = false; // closing quote; reprocess ch below as unquoted
        }
        if (inQuotes) {
          if (ch === '"') quotePending = true;
          else field += ch;
        } else if (ch === '"' && field === '') {
          inQuotes = true;
        } else if (ch === ',') {
          endField();
        } else if (ch === '\n') {
          if (field.endsWith('\r')) field = field.slice(0, -1);
          endRow();
        } else {
          field += ch;
        }
      }
    },
    end() {
      // Tolerate a truncated final record (unterminated quote / missing newline).
      quotePending = false;
      inQuotes = false;
      if (field !== '' || row.length > 0) endRow();
    },
  };
}

/** Run the CSV parser over a Readable stream or a string (tests). */
function parseCsvInput(input, onRow) {
  return new Promise((resolve, reject) => {
    const parser = makeCsvParser(onRow);
    if (typeof input === 'string') {
      try { parser.write(input); parser.end(); resolve(); } catch (e) { reject(e); }
      return;
    }
    if (!input || typeof input.on !== 'function') {
      reject(new Error('fr: unsupported input (expected Readable stream or string)'));
      return;
    }
    const { StringDecoder } = require('string_decoder');
    const decoder = new StringDecoder('utf8');
    input.on('data', (b) => {
      try { parser.write(typeof b === 'string' ? b : decoder.write(b)); }
      catch (e) { input.destroy && input.destroy(); reject(e); }
    });
    input.on('end', () => {
      try { parser.write(decoder.end()); parser.end(); resolve(); } catch (e) { reject(e); }
    });
    input.on('error', reject);
  });
}

/** Header-aware row reader: first row maps names -> indices (BOM-stripped). */
function makeObjectRows(onObj) {
  let header = null;
  return (row) => {
    if (!header) {
      header = row.map((h, i) => (i === 0 ? h.replace(/^﻿/, '') : h).trim());
      return;
    }
    const o = {};
    for (let i = 0; i < header.length; i++) o[header[i]] = row[i] != null ? row[i] : '';
    onObj(o);
  };
}

// ---------------------------------------------------------------------------
// Field mappers
// ---------------------------------------------------------------------------
const truthy = (v) => String(v || '').trim().toLowerCase() === 'true';
const known = (v) => String(v || '').trim() !== '';

/** kW with the watts-published-as-kW repair (22000 -> 22). */
function parseKw(v) {
  const n = parseFloat(String(v || '').replace(',', '.'));
  if (!isFinite(n) || n <= 0) return null;
  const kw = n > 1000 ? n / 1000 : n;
  return Math.round(kw * 10) / 10;
}

const ITINERANCE_RE = /^[A-Z]{2}[A-Z0-9]{4,33}$/;

/** AFIREV itinerance id, uppercased, or null ("Non concerné", blanks, junk). */
function itineranceId(v) {
  const s = String(v || '').trim().toUpperCase();
  return ITINERANCE_RE.test(s) ? s : null;
}

/** [lat, lon] from consolidated_* columns, falling back to coordonneesXY. */
function parseCoords(r) {
  let lat = parseFloat(r.consolidated_latitude);
  let lon = parseFloat(r.consolidated_longitude);
  if (!isFinite(lat) || !isFinite(lon)) {
    const m = String(r.coordonneesXY || '').match(/-?\d+(?:\.\d+)?/g);
    if (m && m.length >= 2) { lon = parseFloat(m[0]); lat = parseFloat(m[1]); }
  }
  if (!isFinite(lat) || !isFinite(lon)) return null;
  if (lat < -90 || lat > 90 || lon < -180 || lon > 180) return null;
  if (lat === 0 && lon === 0) return null;
  return [lat, lon];
}

/** Connector standard from the prise_* booleans (DC beats AC when both). */
function mapStandard(r) {
  if (truthy(r.prise_type_combo_ccs)) return CONNECTOR_STANDARD.CCS;
  if (truthy(r.prise_type_chademo)) return CONNECTOR_STANDARD.CHADEMO;
  if (truthy(r.prise_type_2)) return CONNECTOR_STANDARD.TYPE2;
  return CONNECTOR_STANDARD.UNKNOWN; // EF-only (domestic socket) or unspecified
}

/** Physical format: DC is always tethered; T2 only when the feed says so. */
function mapFormat(r, standard) {
  if (standard === CONNECTOR_STANDARD.CCS || standard === CONNECTOR_STANDARD.CHADEMO) return 'CABLE';
  if (standard === CONNECTOR_STANDARD.TYPE2) {
    const c = String(r.cable_t2_attache || '').trim().toLowerCase();
    if (c === 'true') return 'CABLE';
    if (c === 'false') return 'SOCKET';
    return 'unknown'; // 100% empty in the BETA consolidation today
  }
  if (truthy(r.prise_type_ef)) return 'SOCKET';
  return 'unknown';
}

/** implantation_station -> OCPI-ish parking_type (best-effort). */
function mapParkingType(v) {
  const s = String(v || '').toLowerCase();
  if (s.startsWith('voirie')) return 'ON_STREET';
  if (s.includes('parking')) return 'PARKING_LOT';
  return null; // "Station dédiée à la recharge rapide" has no OCPI equivalent
}

/**
 * Payment/reservation capabilities, same OCPI-style vocabulary as PT/ES so the
 * client's card-payment filter works unchanged. Conservative: tokens are only
 * emitted when paiement_cb is EXPLICITLY known — the client treats a non-empty
 * capability list with no card token as "confirmed no card payment", which must
 * never fire off merely-missing data.
 */
function mapCapabilities(r) {
  const caps = new Set();
  // Emit NOTHING unless payment info is actually known. Every token here — even
  // RESERVABLE, which says nothing about payment — makes the array non-empty, and
  // a non-empty array without a card token is read downstream as CONFIRMED
  // "no card payment" and prunes the site from the Recommended filter. A site with
  // reservation=true but paiement_cb="" must stay unknown, not become a denial.
  if (!known(r.paiement_cb)) return caps;
  if (String(r.paiement_cb).trim().toLowerCase() === 'true') caps.add('CREDIT_CARD_PAYABLE');
  if (truthy(r.paiement_acte)) caps.add('REMOTE_START_STOP_CAPABLE');
  if (truthy(r.reservation)) caps.add('RESERVABLE');
  return caps;
}

/**
 * Dynamic-feed row -> canonical status. Broken beats occupancy; only an explicit
 * "libre" counts as available (same only-confirmed-counts policy as ES).
 */
function mapDynStatus(etat, occupation) {
  if (String(etat).trim() === 'hors_service') return CHARGER_STATUS.OUT_OF_ORDER;
  switch (String(occupation).trim()) {
    case 'libre': return CHARGER_STATUS.AVAILABLE;
    case 'occupe': return CHARGER_STATUS.CHARGING;
    case 'reserve': return CHARGER_STATUS.CHARGING; // booked = not available
    default: return CHARGER_STATUS.UNKNOWN;
  }
}

/** Naive "YYYY-MM-DD HH:MM:SS" (assumed UTC) or ISO-with-offset -> ms epoch. */
function parseHorodatage(s) {
  const t = String(s || '').trim();
  if (!t) return NaN;
  const iso = t.includes('T') ? t : t.replace(' ', 'T');
  const hasZone = /(?:Z|[+-]\d{2}:?\d{2})$/.test(iso);
  return Date.parse(hasZone ? iso : `${iso}Z`);
}

/** Pick a representative site status from its connectors (same as mobie). */
function aggregateStatus(statuses) {
  if (!statuses.length) return CHARGER_STATUS.UNKNOWN;
  if (statuses.includes(CHARGER_STATUS.AVAILABLE)) return CHARGER_STATUS.AVAILABLE;
  if (statuses.includes(CHARGER_STATUS.CHARGING)) return CHARGER_STATUS.CHARGING;
  if (statuses.every((s) => s === CHARGER_STATUS.OUT_OF_ORDER)) return CHARGER_STATUS.OUT_OF_ORDER;
  if (statuses.every((s) => s === CHARGER_STATUS.PLANNED)) return CHARGER_STATUS.PLANNED;
  if (statuses.every((s) => s === CHARGER_STATUS.REMOVED)) return CHARGER_STATUS.REMOVED;
  return statuses.find((s) => s !== CHARGER_STATUS.UNKNOWN) || CHARGER_STATUS.UNKNOWN;
}

// Free-to-charge points advertise an explicit zero ad-hoc tariff (honest data,
// and the client's ad-hoc parser gives them a "0.00" price pin).
const FREE_TARIFF = Object.freeze([{
  type: 'AD_HOC_PAYMENT',
  currency: 'EUR',
  elements: [{ price_components: [{ type: 'ENERGY', price: 0, vat: null, step_size: 1 }] }],
}]);

// ---------------------------------------------------------------------------
// Status feed
// ---------------------------------------------------------------------------
/**
 * Parse the IRVE dynamique CSV into Map<pdcId, {status, lastUpdated}>.
 * Rows older than STALE_HOURS (or unparseable) are DROPPED — absence reads as
 * "unknown" downstream, never as a stale availability claim. When the same pdc
 * appears twice, the freshest row wins.
 *
 * @param {import('stream').Readable|string} input
 */
async function parseStatus(input) {
  const statusById = new Map();
  const tsById = new Map();
  const now = Date.now();
  const staleMs = STALE_HOURS * 3600 * 1000;

  await parseCsvInput(input, makeObjectRows((r) => {
    const id = itineranceId(r.id_pdc_itinerance);
    if (!id) return;
    const ts = parseHorodatage(r.horodatage);
    if (!isFinite(ts) || now - ts > staleMs) return;
    const prev = tsById.get(id);
    if (prev != null && prev >= ts) return;
    tsById.set(id, ts);
    statusById.set(id, {
      status: mapDynStatus(r.etat_pdc, r.occupation_pdc),
      lastUpdated: new Date(ts).toISOString(),
    });
  }));

  return statusById;
}

// ---------------------------------------------------------------------------
// Fetch + normalize
// ---------------------------------------------------------------------------
/**
 * Fetch the feed(s) as Readable streams.
 * @param {{statusOnly?: boolean}} [opts]
 */
async function fetch(opts = {}) {
  const nodeFetch = require('node-fetch');
  const headers = { 'User-Agent': USER_AGENT, Accept: 'text/csv, */*' };

  const statusRes = await nodeFetch(STATUS_URL, { headers });
  if (!statusRes.ok) throw new Error(`fr: status feed HTTP ${statusRes.status} ${statusRes.statusText}`);

  let staticStream = null;
  if (!opts.statusOnly) {
    const staticRes = await nodeFetch(STATIC_URL, { headers });
    if (!staticRes.ok) throw new Error(`fr: static feed HTTP ${staticRes.status} ${staticRes.statusText}`);
    staticStream = staticRes.body;
  }
  return { staticStream, statusStream: statusRes.body };
}

/**
 * Normalize the static CSV (+ optional status join) into common-schema Chargers,
 * grouping charge-point rows into stations. Streaming-friendly: rows are folded
 * into a per-station accumulator as they arrive; only the trimmed station data
 * (not the raw CSV) is held in memory.
 *
 * @param {import('stream').Readable|string} staticInput
 * @param {import('stream').Readable|string|null} statusInput
 * @param {(c: import('../schema').Charger) => void} onCharger
 * @returns {Promise<{sites:number, points:number, statusCount:number}>}
 */
async function normalizeStreaming(staticInput, statusInput, onCharger) {
  const statusById = statusInput ? await parseStatus(statusInput) : new Map();

  /** @type {Map<string, any>} station accumulator keyed by station id */
  const stations = new Map();
  const seenPdc = new Set(); // drop duplicate pdc rows (a handful survive dedup)
  let synth = 0;

  await parseCsvInput(staticInput, makeObjectRows((r) => {
    const coords = parseCoords(r);
    if (!coords) return;
    const [lat, lon] = coords;

    const pdcId = itineranceId(r.id_pdc_itinerance);
    if (pdcId && seenPdc.has(pdcId)) return;
    if (pdcId) seenPdc.add(pdcId);

    const stationId = itineranceId(r.id_station_itinerance)
      || `FRX${String(r.nom_enseigne || 'X').replace(/[^A-Za-z0-9]+/g, '').slice(0, 16).toUpperCase()}${lat.toFixed(4)}A${lon.toFixed(4)}`.replace(/[.-]/g, '_');

    let st = stations.get(stationId);
    if (!st) {
      st = {
        id: stationId,
        lat, lon,
        name: String(r.nom_station || '').trim(),
        address: String(r.adresse_station || '').trim(),
        operator: String(r.nom_operateur || r.nom_enseigne || r.nom_amenageur || '').trim(),
        // Tri-state per schema.js: true = 24/7, false = venue hours only, null =
        // unknown. Starts null; only a row that actually states its hours can move
        // it off null, so an empty `horaires` never asserts "venue hours only".
        open24h: null,
        parkingType: mapParkingType(r.implantation_station),
        caps: new Set(),
        connectors: [],
        lastUpdated: null,
      };
      stations.set(stationId, st);
    }

    // Any 24/7 point makes the site 24/7; otherwise a stated non-24/7 schedule
    // means venue hours. Blank hours leave the flag untouched (unknown).
    const horaires = String(r.horaires || '').trim();
    if (/24\/7/.test(horaires)) st.open24h = true;
    else if (horaires && st.open24h !== true) st.open24h = false;
    for (const cap of mapCapabilities(r)) st.caps.add(cap);
    if (!st.parkingType) st.parkingType = mapParkingType(r.implantation_station);

    const maj = String(r.date_maj || '').trim();
    if (maj && (!st.lastUpdated || maj > st.lastUpdated)) st.lastUpdated = maj;

    const live = pdcId ? statusById.get(pdcId) : null;
    const standard = mapStandard(r);
    st.connectors.push({
      standard,
      format: mapFormat(r, standard),
      chargingMode: null,
      powerKW: parseKw(r.puissance_nominale),
      voltage: null,
      amperage: null,
      pointId: pdcId,
      evseId: pdcId || String(r.id_pdc_local || '').trim() || `${stationId}-${++synth}`,
      status: live ? live.status : CHARGER_STATUS.UNKNOWN,
      // Only a real live observation sets this. Falling back to the producer's
      // date_maj (measured as far back as 2020) would mix years-old dates with
      // fresh ISO ones inside one station, and the client flips an AVAILABLE plug
      // to UNAVAILABLE when it looks >2 weeks staler than its siblings — which
      // would grey out working French chargers. No observation => no timestamp.
      lastUpdated: live ? live.lastUpdated : null,
      ...(truthy(r.gratuit) ? { tariffs: FREE_TARIFF } : {}),
    });
  }));

  let sites = 0;
  let points = 0;
  for (const st of stations.values()) {
    if (st.connectors.length === 0) continue;
    const capabilities = [...st.caps];
    onCharger({
      id: st.id,
      country: COUNTRY,
      source: SOURCE,
      lat: st.lat,
      lon: st.lon,
      name: st.name || st.address || 'Charging Station',
      address: st.address,
      city: '',
      postcode: '',
      operator: st.operator,
      open24h: st.open24h,
      parkingType: st.parkingType || null,
      capabilities,
      connectors: st.connectors,
      tariff: null,
      status: aggregateStatus(st.connectors.map((c) => c.status)),
      lastUpdated: st.lastUpdated || new Date().toISOString(),
    });
    sites++;
    points += st.connectors.length;
  }

  return { sites, points, statusCount: statusById.size };
}

/** Buffered variant (small inputs / tests). */
async function normalize(staticInput, statusInput) {
  const chargers = [];
  await normalizeStreaming(staticInput, statusInput, (c) => chargers.push(c));
  return chargers;
}

module.exports = {
  name: SOURCE,
  country: COUNTRY,
  fetch,
  normalize,
  normalizeStreaming,
  parseStatus,
  hasStatusFeed: true,
  // History MGET-all sweeps over ~47k FR stations are 6x PT's size; roll every 3h
  // (run.js spreads the observation across the 3 hourly buckets it covers).
  historyEveryHours: 3,
  // exported for unit tests
  _makeCsvParser: makeCsvParser,
  _parseKw: parseKw,
  _mapDynStatus: mapDynStatus,
  _parseHorodatage: parseHorodatage,
  _mapCapabilities: mapCapabilities,
  _mapStandard: mapStandard,
  _mapFormat: mapFormat,
  _parseCoords: parseCoords,
};
