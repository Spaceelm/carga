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
// `tarification` — free text, but ~21% of rows are filled and a real grammar
// hides inside. Two shapes dominate:
//   machine-generated: "entre 08:00 et 20:00 : 0.39€ par kwh de charge,
//                       3.2€ par heure d'occupation hors charge, …"
//   hand-written:      "0,29€ / kWh"  ·  "AC 36cts/KWh"  ·  "0.27€/kWh+0.10€/min"
// The rest is prose, URLs, "Inconnu", "-" — those yield nothing and are skipped.
//
// The distinction that matters is WHICH kind of time fee is attached, because a
// per-hour idle penalty does not change what a charge costs while a per-minute
// charging fee does:
//   "…par heure d'occupation…" -> OCPI PARKING_TIME (client ignores it; €/kWh stands)
//   any other €/h or €/min     -> OCPI TIME (client then refuses to show a price,
//                                 which is correct: €/kWh alone would understate)
// Unqualified time fees are treated as TIME on purpose — when the text is
// ambiguous, showing no price beats showing a misleadingly low one.
// ---------------------------------------------------------------------------
// A number that is not itself part of a longer number. The surrounding words decide
// whether a rate is the AC one or the DC one, and whether an hourly fee is an idle
// penalty or part of the charge — but that context is read from the string by INDEX
// after matching, never captured in the pattern: a leading `.{0,28}` group is greedy
// and silently swallows leading digits ("36cts" matching as 6, "0,29€" as 9).
const NUM = '(?<![\\d.,])(\\d+(?:[.,]\\d+)?)';
const RE_KWH = new RegExp(NUM + '\\s*(€|eur|cts?|centimes?)\\s*(?:/|par\\s+)\\s*kwh', 'gi');
const RE_PER_TIME = new RegExp(NUM + '\\s*(?:€|eur)\\s*(?:/|par\\s+)\\s*(min|h\\b|heure)', 'gi');
const RE_SESSION = new RegExp(NUM + '\\s*(?:€|eur)\\s*(?:à\\s+la\\s+connexion|de\\s+connexion|par\\s+(?:session|recharge)|fixe|\\+)', 'gi');
// How much text around a match is inspected to classify it.
const CTX_BEFORE = 30;
const CTX_AFTER = 36;

// Sanity band for a European €/kWh. Rejects unit mix-ups and stray numbers that
// merely sit next to "kWh" (e.g. a "50 kWh" battery-capacity mention).
const MIN_EUR_KWH = 0.05;
const MAX_EUR_KWH = 2.0;

const num = (s) => parseFloat(String(s).replace(',', '.'));

// Which plug kind a fragment is talking about. HPC/ultra/rapide all mean DC in
// French operator copy; "lente"/"accélérée" mean AC.
function kindFromContext(ctx) {
  const s = String(ctx || '').toLowerCase();
  if (/\b(dc|hpc|ultra|rapide|combo|ccs|chademo)\b/.test(s)) return 'dc';
  if (/\b(ac|lente?|accel|accél|t2|type\s*2)\b/.test(s)) return 'ac';
  return null;
}

/**
 * Parse `tarification` into structured rates, preserving the published text.
 *
 * Returns null only when the text is empty. Otherwise always returns
 * `{ raw, rates }`, where `rates` may be empty — an empty list still matters,
 * because the raw text is surfaced to the user as the only pricing information
 * we have for that charger.
 *
 * Each rate is `{ kind, energy, perMinute, sessionFee, idlePerHour }`:
 *   kind        'ac' | 'dc' | null (null = applies to any plug)
 *   energy      €/kWh
 *   perMinute   €/min charged DURING the session (an OCPI TIME component)
 *   sessionFee  one-off connection fee (OCPI FLAT)
 *   idlePerHour €/h for overstaying AFTER charging (OCPI PARKING_TIME) — a
 *               penalty, deliberately excluded from the cost of a normal charge
 *
 * Nothing is min()'d away here: the AC and DC rates stay separate so each plug can
 * be priced with its own, and the time components are kept so the client can
 * amortize them over its reference session instead of discarding the price.
 */
function parseTariff(text) {
  const raw = String(text || '').trim();
  if (!raw) return null;

  /** @type {Map<string, any>} kind -> rate */
  const byKind = new Map();
  const rateFor = (kind) => {
    const key = kind || '*';
    if (!byKind.has(key)) byKind.set(key, { kind: kind || null, energy: null, perMinute: null, sessionFee: null, idlePerHour: null });
    return byKind.get(key);
  };

  const before = (i) => raw.slice(Math.max(0, i - CTX_BEFORE), i);
  const after = (i) => raw.slice(i, i + CTX_AFTER);

  let m;
  RE_KWH.lastIndex = 0;
  while ((m = RE_KWH.exec(raw)) !== null) {
    const unit = m[2].toLowerCase();
    let v = num(m[1]);
    if (unit.startsWith('c')) v /= 100; // centimes
    if (!isFinite(v) || v < MIN_EUR_KWH || v > MAX_EUR_KWH) continue;
    const r = rateFor(kindFromContext(before(m.index)));
    // Several windows quoted for one plug kind (time-of-day pricing): keep the
    // lowest, which is the "from €X" the rest of the app already shows.
    if (r.energy == null || v < r.energy) r.energy = v;
  }

  RE_PER_TIME.lastIndex = 0;
  while ((m = RE_PER_TIME.exec(raw)) !== null) {
    const v = num(m[1]);
    if (!isFinite(v) || v <= 0) continue;
    const perMinuteUnit = /^min/i.test(m[2]);
    const perMin = perMinuteUnit ? v : v / 60;
    // "occupation hors charge" and friends are penalties for overstaying AFTER the
    // charge, so they must not inflate the cost of a normal session.
    const idle = /occupation|stationnement|au-del|apr[eè]s|surstationnement/i
      .test(after(m.index + m[0].length));
    const r = rateFor(kindFromContext(before(m.index)));
    if (idle) {
      const perHour = perMinuteUnit ? v * 60 : v;
      if (r.idlePerHour == null || perHour < r.idlePerHour) r.idlePerHour = perHour;
    } else if (r.perMinute == null || perMin < r.perMinute) {
      r.perMinute = perMin;
    }
  }

  RE_SESSION.lastIndex = 0;
  while ((m = RE_SESSION.exec(raw)) !== null) {
    const v = num(m[1]);
    if (!isFinite(v) || v <= 0 || v > 20) continue;
    const r = rateFor(null);
    if (r.sessionFee == null || v < r.sessionFee) r.sessionFee = v;
  }

  // A fee stated without its own €/kWh belongs to the generic rate; drop kind
  // buckets that ended up carrying nothing at all.
  const rates = [...byKind.values()].filter(
    (r) => r.energy != null || r.perMinute != null || r.sessionFee != null || r.idlePerHour != null,
  );
  return { raw, rates };
}

/**
 * The rate that applies to one plug: its own kind's, else the generic one, merged
 * so a generic session fee still applies to a kind-specific energy rate.
 */
function rateForConnector(parsed, isDc) {
  if (!parsed || !parsed.rates.length) return null;
  const want = isDc ? 'dc' : 'ac';
  const specific = parsed.rates.find((r) => r.kind === want) || null;
  const generic = parsed.rates.find((r) => r.kind === null) || null;
  if (!specific) return generic;
  return {
    kind: specific.kind,
    energy: specific.energy != null ? specific.energy : (generic && generic.energy),
    perMinute: specific.perMinute != null ? specific.perMinute : (generic && generic.perMinute),
    sessionFee: specific.sessionFee != null ? specific.sessionFee : (generic && generic.sessionFee),
    idlePerHour: specific.idlePerHour != null ? specific.idlePerHour : (generic && generic.idlePerHour),
  };
}

/** Structured rate -> OCPI AD_HOC_PAYMENT tariff, or null when it prices nothing. */
function rateToTariff(rate) {
  if (!rate || rate.energy == null) return null;
  const pc = [{ type: 'ENERGY', price: Math.round(rate.energy * 10000) / 10000, vat: null, step_size: 1 }];
  if (rate.perMinute) pc.push({ type: 'TIME', price: Math.round(rate.perMinute * 10000) / 10000, vat: null, step_size: 60 });
  if (rate.sessionFee) pc.push({ type: 'FLAT', price: rate.sessionFee, vat: null, step_size: 1 });
  if (rate.idlePerHour) pc.push({ type: 'PARKING_TIME', price: rate.idlePerHour, vat: null, step_size: 60 });
  return [{ type: 'AD_HOC_PAYMENT', currency: 'EUR', elements: [{ price_components: pc }] }];
}

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
    const isDc = standard === CONNECTOR_STANDARD.CCS || standard === CONNECTOR_STANDARD.CHADEMO;
    // Price this plug with its OWN kind's rate, so narrowing the map to AC or DC
    // shows the rate that actually applies instead of the site's cheapest.
    const parsedTariff = truthy(r.gratuit) ? null : parseTariff(r.tarification);
    const tariffs = truthy(r.gratuit)
      ? FREE_TARIFF
      : rateToTariff(rateForConnector(parsedTariff, isDc));
    st.connectors.push({
      standard,
      format: mapFormat(r, standard),
      chargingMode: null,
      powerKW: parseKw(r.puissance_nominale),
      voltage: null,
      amperage: null,
      pointId: pdcId,
      // Always starts with "FR": the client derives a charger's country from this
      // id prefix to scope community tariffs, and an id that doesn't identify its
      // country falls through to being priced with another country's tariffs. The
      // raw id_pdc_local is deliberately NOT used bare — producers set it to
      // arbitrary local strings. stationId is itself always FR-prefixed (real
      // AFIREV id or the FRX… synthetic), so deriving from it preserves the rule.
      evseId: pdcId || `${stationId}-${++synth}`,
      status: live ? live.status : CHARGER_STATUS.UNKNOWN,
      // Only a real live observation sets this. Falling back to the producer's
      // date_maj (measured as far back as 2020) would mix years-old dates with
      // fresh ISO ones inside one station, and the client flips an AVAILABLE plug
      // to UNAVAILABLE when it looks >2 weeks staler than its siblings — which
      // would grey out working French chargers. No observation => no timestamp.
      lastUpdated: live ? live.lastUpdated : null,
      // gratuit wins: an explicit "free" flag beats whatever the prose says.
      ...(tariffs ? { tariffs } : {}),
      // The operator's published wording, verbatim, whenever there is any. Kept even
      // when nothing parsed — for a charger with no computable price this text is the
      // only pricing information that exists, and the UI shows it rather than nothing.
      ...(parsedTariff && parsedTariff.raw ? { tariffNote: parsedTariff.raw } : {}),
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
  _parseTariff: parseTariff,
  _rateForConnector: rateForConnector,
  _rateToTariff: rateToTariff,
  _mapStandard: mapStandard,
  _mapFormat: mapFormat,
  _parseCoords: parseCoords,
};
