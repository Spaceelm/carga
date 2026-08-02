/**
 * providers/reve.js
 *
 * Spain STATIC charger provider using REVE's official external API (Red Eléctrica
 * de España — https://www.mapareve.es/docs/api/external/v1). Replaces the DGT
 * DATEX II static feed with the richer OCPI registry (connectors, power, standards,
 * CPO names).
 *
 * STATIC ONLY (hasStatusFeed=false): live occupancy for ES still comes from the
 * request-time REVE markers overlay in the serving function (netlify/functions/
 * _reve.js + mobie-charger-location.js). The official bulk feed only exposes a
 * boolean operational_status (working/broken), not free/occupied, so we don't use
 * it for live status here.
 *
 * Auth: every request needs `x-api-key: <REVE_API_KEY>` (env var; never committed).
 * The API is paginated (page/limit) and rate-limited — we pace + back off.
 *
 * ToS: non-commercial use only; attribute "Red Eléctrica de España" as the source.
 */

const { CHARGER_STATUS, CONNECTOR_STANDARD } = require('../schema');

const SOURCE = 'reve';
const COUNTRY = 'ES';
const BASE = 'https://www.mapareve.es/api/external/v1';
const PAGE_LIMIT = 100;
const PAGE_DELAY_MS = 400; // polite pacing between pages
const MAX_RETRIES = 6;

/** Read the API key from env, or throw with a clear message. */
function apiKey() {
  const k = process.env.REVE_API_KEY;
  if (!k) {
    throw new Error(
      'reve: REVE_API_KEY env var is required (REVE official API key). ' +
        'Set it as a repo/CI secret; never hardcode it.'
    );
  }
  return k;
}

function headers() {
  return {
    'User-Agent': 'carregar-ingest/1.0 (+https://carregar.netlify.app)',
    Accept: 'application/json',
    'x-api-key': apiKey(),
  };
}

const sleep = (ms) => new Promise((r) => setTimeout(r, ms));

/** Map an OCPI connector standard onto our canonical CONNECTOR_STANDARD enum. */
function mapStandard(ocpi) {
  switch (String(ocpi || '').toUpperCase()) {
    case 'IEC_62196_T2':
      return CONNECTOR_STANDARD.TYPE2;
    case 'IEC_62196_T2_COMBO':
    case 'IEC_62196_T1_COMBO':
      return CONNECTOR_STANDARD.CCS;
    case 'IEC_62196_T1':
      return CONNECTOR_STANDARD.TYPE1;
    case 'IEC_62196_T3A':
    case 'IEC_62196_T3C':
      return CONNECTOR_STANDARD.TYPE3;
    case 'CHADEMO':
      return CONNECTOR_STANDARD.CHADEMO;
    case 'TESLA_R':
    case 'TESLA_S':
      return CONNECTOR_STANDARD.TESLA;
    default:
      return CONNECTOR_STANDARD.UNKNOWN;
  }
}

const numOrNull = (v) => {
  const n = typeof v === 'number' ? v : parseFloat(v);
  return Number.isFinite(n) ? n : null;
};

/**
 * Fetch one page of a paginated array endpoint (e.g. /locations or
 * /connectors/tariffs), retrying on rate-limit / transient errors with
 * exponential backoff. Returns the parsed array (possibly empty).
 */
async function getPage(nodeFetch, path, page) {
  const url = `${BASE}${path}?page=${page}&limit=${PAGE_LIMIT}`;
  for (let attempt = 0; attempt <= MAX_RETRIES; attempt++) {
    let res, text;
    try {
      res = await nodeFetch(url, { headers: headers() });
      text = await res.text();
    } catch (err) {
      // network blip — back off and retry
      await sleep(Math.min(30000, 1000 * 2 ** attempt));
      continue;
    }
    if (res.ok && text && text.trimStart().startsWith('[')) {
      try {
        return JSON.parse(text);
      } catch (_e) {
        /* fall through to retry */
      }
    }
    // 429 / "Retry later" / any non-array 200 -> treat as rate-limited/transient.
    if (res.status === 429 || /retry later/i.test(text || '') || !res.ok || attempt < MAX_RETRIES) {
      await sleep(Math.min(30000, 1000 * 2 ** attempt));
      continue;
    }
    throw new Error(`reve: ${path} page ${page} HTTP ${res.status}: ${(text || '').slice(0, 120)}`);
  }
  throw new Error(`reve: ${path} page ${page} still rate-limited after ${MAX_RETRIES} retries.`);
}

/** Paginate a whole array endpoint into one array (stops on short/empty page). */
async function getAllPages(nodeFetch, path) {
  const all = [];
  let page = 1;
  // eslint-disable-next-line no-constant-condition
  while (true) {
    const batch = await getPage(nodeFetch, path, page);
    if (!Array.isArray(batch) || batch.length === 0) break;
    all.push(...batch);
    if (batch.length < PAGE_LIMIT) break; // last page
    page++;
    await sleep(PAGE_DELAY_MS);
  }
  return all;
}

// Pages to fetch per resumable-crawl run. REVE throttles /locations to ~5 req/hr,
// so a full (~120-page) crawl can't complete in one run — we take a small chunk each
// run and resume from a Redis checkpoint. 4 leaves headroom under the hourly cap.
const MAX_PAGES_PER_RUN = 4;

/**
 * Fetch one /locations page WITHOUT throwing on throttle: retries only true network
 * blips (briefly), and reports rate-limit/HTTP errors as a signal so the crawl can
 * checkpoint and pick up next run instead of aborting the whole ES ingest.
 * @returns {Promise<{data?:object[], rateLimited?:boolean, error?:string}>}
 */
async function getPageGraceful(nodeFetch, path, page) {
  for (let attempt = 0; attempt < 2; attempt++) {
    let res, text;
    try {
      res = await nodeFetch(`${BASE}${path}?page=${page}&limit=${PAGE_LIMIT}`, { headers: headers() });
      text = await res.text();
    } catch (_e) {
      await sleep(1000); // transient network blip — one quick retry
      continue;
    }
    if (res.ok && text && text.trimStart().startsWith('[')) {
      try { return { data: JSON.parse(text) }; } catch (_e) { return { error: 'unparseable body' }; }
    }
    if (res.status === 429 || /retry later/i.test(text || '')) return { rateLimited: true };
    return { error: `HTTP ${res.status}: ${(text || '').slice(0, 120)}` };
  }
  return { rateLimited: true }; // treat persistent transient failure as throttle
}

/**
 * Resumable /locations crawl. REVE limits /locations to ~5 req/hr — far too few to
 * fetch the whole registry in one run. So we crawl in small chunks: each run fetches
 * up to `maxPages`, stages every page under CRAWL_PAGE_KEY, and checkpoints the next
 * page in CRAWL_KEY. Only once the final (short) page is reached do we reassemble all
 * staged pages and return the complete locations array for a single upsert+sweep —
 * so the previous dataset (e.g. the DGT snapshot) keeps serving until the crawl is
 * whole (no partial writes, no duplicate markers mid-crawl).
 *
 * @param {import('@upstash/redis').Redis} redis
 * @param {{keys:object, log?:Function, maxPages?:number, dryRun?:boolean}} opts
 * @returns {Promise<{complete:boolean, nextPage:number, fetched:number, locations:object[]|null, lastPage?:number, dryRun?:boolean}>}
 */
async function crawlStatic(redis, opts = {}) {
  const { keys, log = () => {}, maxPages = MAX_PAGES_PER_RUN, dryRun = false } = opts;
  const nodeFetch = opts.nodeFetch || require('node-fetch');
  const cc = COUNTRY.toLowerCase();

  const stateRaw = await redis.get(keys.CRAWL_KEY(cc));
  const state = stateRaw ? (typeof stateRaw === 'string' ? JSON.parse(stateRaw) : stateRaw) : null;
  let nextPage = state && Number.isFinite(state.nextPage) ? state.nextPage : 1;
  const startedAt = (state && state.startedAt) || new Date().toISOString();

  let fetched = 0;
  let complete = false;
  while (fetched < maxPages) {
    const r = await getPageGraceful(nodeFetch, '/locations', nextPage);
    if (r.rateLimited) { log(`reve crawl: throttled at page ${nextPage} — checkpointing (resumes next run).`); break; }
    if (r.error) { log(`reve crawl: page ${nextPage} error (${r.error}) — checkpointing.`); break; }
    const batch = Array.isArray(r.data) ? r.data : [];
    if (!dryRun) await redis.set(keys.CRAWL_PAGE_KEY(cc, nextPage), JSON.stringify(batch));
    fetched++;
    if (batch.length < PAGE_LIMIT) { complete = true; nextPage++; break; } // final page
    nextPage++;
    await sleep(PAGE_DELAY_MS);
  }

  if (dryRun) {
    log(`[dry-run] reve crawl: fetched ${fetched} page(s) from page ${state ? state.nextPage || 1 : 1}, complete=${complete} (nothing staged).`);
    return { complete: false, nextPage, fetched, locations: null, dryRun: true };
  }

  if (!complete) {
    await redis.set(keys.CRAWL_KEY(cc), JSON.stringify({ nextPage, startedAt }));
    log(`reve crawl: staged ${fetched} page(s); next page ${nextPage}.`);
    return { complete: false, nextPage, fetched, locations: null };
  }

  // Final page reached — reassemble every staged page into one locations array.
  const lastPage = nextPage - 1;
  const locations = [];
  for (let p = 1; p <= lastPage; p++) {
    const raw = await redis.get(keys.CRAWL_PAGE_KEY(cc, p));
    const arr = raw ? (typeof raw === 'string' ? JSON.parse(raw) : raw) : null;
    if (Array.isArray(arr)) locations.push(...arr);
  }
  log(`reve crawl: COMPLETE — ${lastPage} page(s), ${locations.length} locations; writing + sweeping.`);
  return { complete: true, nextPage, fetched, locations, lastPage };
}

/** Delete the crawl checkpoint + all staged pages after a completed cycle is written. */
async function clearCrawlState(redis, keys, lastPage) {
  const cc = COUNTRY.toLowerCase();
  const pageKeys = [];
  for (let p = 1; p <= lastPage; p++) pageKeys.push(keys.CRAWL_PAGE_KEY(cc, p));
  const BATCH = 200;
  for (let i = 0; i < pageKeys.length; i += BATCH) await redis.del(...pageKeys.slice(i, i + BATCH));
  await redis.del(keys.CRAWL_KEY(cc));
}

/**
 * Normalize a REVE OCPI Tariff into the shape the frontend's ad-hoc price parser
 * already understands (see 01-group_similar_stations.js): a tariff of type
 * "AD_HOC_PAYMENT" with elements[].price_components[] whose ENERGY/TIME prices are
 * NUMBERS (REVE sends them as strings). This lets ES prices flow through the exact
 * same pipeline as MOBI.E ad-hoc tariffs — no client parser change needed.
 */
function reveTariffToOcpi(tariff) {
  if (!tariff || typeof tariff !== 'object') return null;
  const elements = (tariff.elements || []).map((el) => ({
    price_components: (el.price_components || []).map((pc) => ({
      type: pc.type,
      price: numOrNull(pc.price),
      vat: numOrNull(pc.vat),
      step_size: pc.step_size,
    })),
    restrictions: el.restrictions || undefined,
  }));
  return {
    id: tariff.id,
    type: 'AD_HOC_PAYMENT',
    currency: tariff.currency || undefined,
    elements,
  };
}

/**
 * Fetch and index /connectors/tariffs into a Map keyed by BOTH connector_id and
 * evse_id → array of normalized tariffs. Graceful: any failure returns an empty
 * Map so locations + hours still ingest (tariffs just stay empty that run).
 */
async function fetchTariffs(nodeFetch) {
  const byId = new Map();
  let rows;
  try {
    rows = await getAllPages(nodeFetch, '/connectors/tariffs');
  } catch (err) {
    console.warn(`reve: /connectors/tariffs unavailable, ingesting without prices: ${err.message}`);
    return byId;
  }
  for (const row of rows) {
    const tariffs = (row.tariffs || []).map(reveTariffToOcpi).filter(Boolean);
    if (tariffs.length === 0) continue;
    if (row.connector_id) byId.set(row.connector_id, tariffs);
    if (row.evse_id && !byId.has(row.evse_id)) byId.set(row.evse_id, tariffs);
  }
  return byId;
}

/**
 * Fetch and index /evses/operational_status into a Map keyed by evse_id -> boolean
 * (true = operational, false = broken/out-of-service). Graceful: any failure returns
 * an empty Map so locations still ingest. Rate-limited, so a run may only fill part
 * of the registry — coverage accumulates across runs.
 */
async function fetchOperationalStatus(nodeFetch) {
  const byId = new Map();
  let rows;
  try {
    rows = await getAllPages(nodeFetch, '/evses/operational_status');
  } catch (err) {
    console.warn(`reve: /evses/operational_status unavailable, ingesting without broken flags: ${err.message}`);
    return byId;
  }
  for (const row of rows) {
    if (row && row.evse_id != null && typeof row.operational_status === 'boolean') {
      byId.set(row.evse_id, row.operational_status);
    }
  }
  return byId;
}

/**
 * Provider fetch. Static-only: paginate the full ES /locations registry, plus two
 * graceful passes — dynamic pricing (/connectors/tariffs) and operational status
 * (/evses/operational_status). status-only runs go through runAreaStatus() instead.
 * @returns {Promise<{staticStream: object|null, statusStream: null}>}
 */
async function fetch(opts = {}) {
  if (opts.statusOnly) return { staticStream: null, statusStream: null };
  const nodeFetch = require('node-fetch');
  const locations = await getAllPages(nodeFetch, '/locations');
  const { tariffsById, operationalById } = await fetchEnrichment();
  return { staticStream: { locations, tariffsById, operationalById }, statusStream: null };
}

/**
 * The two secondary passes that enrich a location set: dynamic pricing
 * (/connectors/tariffs) and operational status (/evses/operational_status).
 *
 * Split out of fetch() because ES does NOT go through fetch(): /locations is
 * rate-limited well below one-run coverage, so ES is ingested by the resumable
 * crawl instead, which only ever collected locations. That left both of these
 * unreachable for the one country that uses them — every ES connector shipped
 * with `tariffs: []`. run.js now calls this once the crawl completes, i.e. once
 * per crawl cycle rather than on every chunk, which keeps it inside the API's
 * ~5 req/hr budget.
 *
 * Graceful by construction: either pass failing yields an empty Map, so a
 * rate-limited run still writes locations rather than losing the whole cycle.
 */
async function fetchEnrichment() {
  const nodeFetch = require('node-fetch');
  const tariffsById = await fetchTariffs(nodeFetch);
  const operationalById = await fetchOperationalStatus(nodeFetch);
  return { tariffsById, operationalById };
}

/**
 * Derive opening state from an OCPI Location.
 *   open24h (chargeable around the clock — what the 24/7 filter keys on):
 *     opening_times.twentyfourseven === true  -> true  (venue open 24/7)
 *     charging_when_closed === true           -> true  (charger usable even when the venue is shut)
 *     else non-empty regular_hours[]          -> false (only during venue hours)
 *     else                                    -> null  (unknown)
 *   chargingWhenClosed: true ONLY in the "24/7 charger but the venue itself may be
 *     closed" case, so the UI can flag that on-site amenities might be unavailable.
 * Matches the MOBI.E semantics and the frontend 24/7 filter (which keeps === true).
 */
function openStateFromLocation(loc) {
  const ot = loc && loc.opening_times;
  const twentyfourseven = !!(ot && ot.twentyfourseven === true);
  const hasRegular = !!(ot && Array.isArray(ot.regular_hours) && ot.regular_hours.length > 0);
  const whenClosed = !!(loc && loc.charging_when_closed === true);
  let open24h;
  if (twentyfourseven || whenClosed) open24h = true;
  else if (hasRegular) open24h = false;
  else open24h = null;
  const chargingWhenClosed = open24h === true && !twentyfourseven && whenClosed;
  return { open24h, chargingWhenClosed };
}

/**
 * Map one OCPI Location object to a common-schema Charger (or null if unusable).
 * @param {object} loc            OCPI Location
 * @param {Map<string,object[]>} [tariffsById]  connector_id/evse_id -> normalized tariffs
 * @param {Map<string,boolean>} [operationalById]  evse_id -> operational (false = broken)
 */
function locationToCharger(loc, tariffsById, operationalById) {
  if (!loc || typeof loc !== 'object') return null;
  const coords = loc.coordinates || {};
  const lat = numOrNull(coords.latitude);
  const lon = numOrNull(coords.longitude);
  if (lat == null || lon == null) return null;

  const tariffs = tariffsById instanceof Map ? tariffsById : null;
  const operational = operationalById instanceof Map ? operationalById : null;
  const connectors = [];
  // EVSE capabilities are per-EVSE; collapse to a site-level union (deduped) since
  // the app shows one marker/detail per site. Stored raw (REVE types it as free
  // strings) so no token is dropped; the UI maps known ones to friendly labels.
  const capabilities = new Set();
  for (const evse of loc.evses || []) {
    for (const cap of (evse && evse.capabilities) || []) {
      if (typeof cap === 'string' && cap.trim()) capabilities.add(cap.trim());
    }
    for (const conn of (evse && evse.connectors) || []) {
      const pointId = conn.id || evse.evse_id;
      const evseId = evse.evse_id || conn.id;
      const connTariffs = tariffs ? tariffs.get(pointId) || tariffs.get(evseId) || null : null;
      // Known-broken EVSEs (operational_status === false) render out-of-order; the
      // live /markers overlay still overrides with real-time status when it matches.
      const isBroken = operational ? operational.get(evseId) === false : false;
      connectors.push({
        pointId,
        evseId,
        standard: mapStandard(conn.standard),
        format: conn.format || 'SOCKET',
        // OCPI max_electric_power is WATTS; schema stores kW.
        powerKW: numOrNull(conn.max_electric_power) != null ? numOrNull(conn.max_electric_power) / 1000 : null,
        voltage: numOrNull(conn.max_voltage),
        amperage: numOrNull(conn.max_amperage),
        // Static registry: no live occupancy. A known-broken unit is out-of-order;
        // otherwise unknown (the serving REVE overlay applies live occupancy).
        status: isBroken ? CHARGER_STATUS.OUT_OF_ORDER : CHARGER_STATUS.UNKNOWN,
        lastUpdated: conn.last_static_updated || (evse && evse.last_static_updated) || null,
        // Dynamic pricing from /connectors/tariffs, normalized to the frontend's
        // ad-hoc tariff shape. Omitted when we have none for this connector.
        ...(connTariffs ? { tariffs: connTariffs } : {}),
      });
    }
  }
  if (connectors.length === 0) return null;

  const openState = openStateFromLocation(loc);
  return {
    id: loc.id,
    country: COUNTRY,
    source: SOURCE,
    lat,
    lon,
    name: loc.name || loc.address || 'Charging Station',
    address: loc.address || '',
    city: loc.city || '',
    operator: loc.cpo_name || loc.party_id || '',
    open24h: openState.open24h,
    // Only set when 24/7 is due to "charging while venue closed" (not a true 24/7
    // venue) — lets the UI note the venue itself may be shut.
    ...(openState.chargingWhenClosed ? { chargingWhenClosed: true } : {}),
    facilities: Array.isArray(loc.facilities) ? loc.facilities : [],
    parkingType: loc.parking_type || null,
    capabilities: [...capabilities],
    status: CHARGER_STATUS.UNKNOWN,
    connectors,
    lastUpdated: loc.last_updated || null,
  };
}

/**
 * Streaming normalize: invoke onCharger per station.
 * @param {Array<object>} staticInput  locations array from fetch()
 * @param {*} _statusInput             unused (static-only)
 * @param {(c:import('../schema').Charger)=>(void|Promise<void>)} onCharger
 * @returns {Promise<{sites:number, points:number, statusCount:number}>}
 */
async function normalizeStreaming(staticInput, _statusInput, onCharger) {
  let sites = 0;
  let points = 0;
  // fetch() returns { locations, tariffsById }; tolerate a bare locations array too.
  const locs = Array.isArray(staticInput)
    ? staticInput
    : (staticInput && Array.isArray(staticInput.locations) ? staticInput.locations : []);
  const tariffsById = staticInput && staticInput.tariffsById instanceof Map ? staticInput.tariffsById : null;
  const operationalById = staticInput && staticInput.operationalById instanceof Map ? staticInput.operationalById : null;
  for (const loc of locs) {
    const charger = locationToCharger(loc, tariffsById, operationalById);
    if (!charger) continue;
    points += charger.connectors.length;
    onCharger(charger);
    sites++;
  }
  return { sites, points, statusCount: 0 };
}

async function normalize(staticInput) {
  const chargers = [];
  await normalizeStreaming(staticInput, null, (c) => chargers.push(c));
  return chargers;
}

/** No bulk id-keyed status feed; ES status is an area sweep (see runAreaStatus). */
async function parseStatus() {
  return new Map();
}

// ---- ES live-status sweep (builds the availability history the Min Availability
// filter reads). REVE has no bulk status feed, but its public /markers endpoint
// returns per-site live status for a bbox. We tile populated Spain, match markers to
// our stations by proximity, set status + roll the 7×24 history. Map freshness still
// comes from the request-time overlay; this sweep exists to accumulate history. ----

const TILE_DEG = 0.25; // ~28 km tiles → REVE returns unclustered markers
const MATCH_RADIUS_M = 60; // station↔marker match slop (same as the serving overlay)
const SWEEP_PACE_MS = 120; // pacing between a worker's successive /markers POSTs
const SWEEP_CONCURRENCY = 4; // parallel workers (bounded to stay polite to /markers)

function haversineM(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const R = 6371000;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return R * 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
}

function nearestMarker(lat, lon, markers, radiusM) {
  let best = null;
  let bestD = Infinity;
  for (const m of markers) {
    const d = haversineM(lat, lon, m.lat, m.lon);
    if (d < bestD) { bestD = d; best = m; }
  }
  return best && bestD <= radiusM ? best : null;
}

/** REVE/OCPI status token -> canonical connector status (for history + baseline). */
function reveOcpiToCanonical(ocpi) {
  switch (ocpi) {
    case 'AVAILABLE': return CHARGER_STATUS.AVAILABLE;
    case 'CHARGING': return CHARGER_STATUS.CHARGING;
    case 'OUTOFORDER': return CHARGER_STATUS.OUT_OF_ORDER;
    case 'UNAVAILABLE': return CHARGER_STATUS.CHARGING; // occupied/blocked → not available
    default: return null;
  }
}

function parseJson(raw) {
  if (raw == null) return null;
  if (typeof raw === 'string') { try { return JSON.parse(raw); } catch (_e) { return null; } }
  return raw;
}

/**
 * Status-only sweep for ES. Gets station coords from the coordindex blob (one GET;
 * falls back to MGET-all if absent), tiles populated Spain, fetches REVE /markers
 * per tile, matches to stations, and — only for stations with a real marker match —
 * reads that record, sets status + accumulates history, and writes it back if
 * anything changed. Unmatched stations are never touched (no false "unavailable").
 * Reading only matched records (not all of ES) keeps the Redis command count low.
 *
 * @param {object} redis    Upstash client (created by run.js)
 * @param {object} helpers  { keys, accumulateHistory, log, dryRun }
 * @returns {Promise<{upserted:number, statuses:number, dryRun:boolean}>}
 */
async function runAreaStatus(redis, helpers) {
  const { keys, accumulateHistory, log = () => {}, dryRun = false } = helpers;
  const cc = COUNTRY.toLowerCase();
  const { fetchReveStatus, reveStatusToOcpi } = require('../../../netlify/functions/_reve');
  const BATCH = 200;

  // Coord source: prefer the coordindex blob (1 GET, seeded by the full refresh);
  // else MGET-all records once (fallback) and cache them for write-back.
  const coords = new Map(); // id -> [lat, lon]
  const recordCache = new Map(); // id -> parsed record (only when we MGET)
  // MUST use chunkedGet: store.js writes this index with chunkedSet, so once ES
  // outgrows the chunk threshold a plain GET returns the {"__chunks":N} sentinel —
  // which is a truthy object and would silently yield ZERO coords (no fallback,
  // no error, sweep does nothing). chunkedGet returns null for a missing/broken
  // index, which correctly falls through to the MGET-all path below.
  const { chunkedGet } = require('../store');
  const coordIdx = await chunkedGet(redis, keys.COORDINDEX_KEY(cc));
  if (coordIdx && typeof coordIdx === 'object') {
    for (const [id, ll] of Object.entries(coordIdx)) {
      if (Array.isArray(ll) && isFinite(ll[0]) && isFinite(ll[1])) coords.set(id, ll);
    }
  } else {
    const ids = await redis.smembers(keys.IDS_KEY(cc));
    if (!ids || ids.length === 0) {
      throw new Error('reve: no ES ids in Redis. Run a full refresh first (omit --status-only).');
    }
    for (let i = 0; i < ids.length; i += BATCH) {
      const slice = ids.slice(i, i + BATCH);
      const jsons = await redis.mget(...slice.map((id) => keys.CHARGER_KEY(cc, id)));
      for (let j = 0; j < slice.length; j++) {
        const rec = parseJson(jsons[j]);
        if (rec && isFinite(rec.lat) && isFinite(rec.lon)) {
          coords.set(slice[j], [rec.lat, rec.lon]);
          recordCache.set(slice[j], rec);
        }
      }
    }
  }

  // Group into ~28 km tiles; only tiles that contain a station are fetched.
  const tiles = new Map();
  for (const [id, [lat, lon]] of coords) {
    const key = `${Math.floor(lat / TILE_DEG)}:${Math.floor(lon / TILE_DEG)}`;
    if (!tiles.has(key)) {
      tiles.set(key, { tlat: Math.floor(lat / TILE_DEG), tlon: Math.floor(lon / TILE_DEG), items: [] });
    }
    tiles.get(key).items.push({ id, lat, lon });
  }
  log(`reve status: ${coords.size} ES stations across ${tiles.size} tiles`);

  // Fetch markers per tile (bounded-parallel: SWEEP_CONCURRENCY workers pull from a
  // shared queue, each pacing its own requests), resolve the desired status for matched
  // stations only. Parallelism cuts the sweep from ~9 min sequential to a couple of
  // minutes so it fits the Actions-minutes budget, while staying polite to /markers.
  const desired = new Map(); // id -> canonical status
  const tileList = [...tiles.values()];
  let ti = 0; // shared cursor; ti++ is atomic between awaits in single-threaded JS
  const worker = async () => {
    while (ti < tileList.length) {
      const tile = tileList[ti++];
      const bbox = {
        minLat: tile.tlat * TILE_DEG,
        maxLat: (tile.tlat + 1) * TILE_DEG,
        minLon: tile.tlon * TILE_DEG,
        maxLon: (tile.tlon + 1) * TILE_DEG,
      };
      // No redis arg → skip the 60 s overlay cache (pointless for a sweep, saves commands).
      let markers = [];
      try { markers = await fetchReveStatus(bbox); } catch (_e) { markers = []; }
      if (markers.length) {
        for (const it of tile.items) {
          const m = nearestMarker(it.lat, it.lon, markers, MATCH_RADIUS_M);
          const canonical = m ? reveOcpiToCanonical(reveStatusToOcpi(m.status)) : null;
          if (canonical) desired.set(it.id, canonical);
        }
      }
      await sleep(SWEEP_PACE_MS);
    }
  };
  await Promise.all(Array.from({ length: Math.min(SWEEP_CONCURRENCY, tileList.length) }, worker));
  log(`reve status: matched ${desired.size}/${coords.size} stations to a live marker`);

  if (dryRun) {
    log(`[dry-run] would update ${desired.size} ES stations (no writes)`);
    return { upserted: 0, statuses: desired.size, dryRun: true };
  }

  // Read (only) the matched records not already cached, patch status + roll history,
  // and MSET only the ones that actually changed.
  const now = new Date();
  const matchedIds = [...desired.keys()];
  let written = 0;
  for (let i = 0; i < matchedIds.length; i += BATCH) {
    const slice = matchedIds.slice(i, i + BATCH);
    const need = slice.filter((id) => !recordCache.has(id));
    if (need.length) {
      const jsons = await redis.mget(...need.map((id) => keys.CHARGER_KEY(cc, id)));
      need.forEach((id, k) => { const r = parseJson(jsons[k]); if (r) recordCache.set(id, r); });
    }
    const updates = {};
    for (const id of slice) {
      const rec = recordCache.get(id);
      if (!rec) continue;
      const canonical = desired.get(id);
      let ch = false;
      for (const conn of rec.connectors || []) {
        if (conn.status !== canonical) {
          // Session start = "last used" (REVE maps charging/occupied → CHARGING).
          if (canonical === CHARGER_STATUS.CHARGING) conn.lastBusyAt = now.toISOString();
          conn.status = canonical;
          ch = true;
        }
      }
      const histChanged = accumulateHistory(rec, now);
      if (ch) rec.lastUpdated = new Date().toISOString();
      if (ch || histChanged) { updates[keys.CHARGER_KEY(cc, id)] = JSON.stringify(rec); written++; }
    }
    if (Object.keys(updates).length) await redis.mset(updates);
  }

  await redis.set(
    keys.META_KEY(cc),
    JSON.stringify({ lastRun: new Date().toISOString(), count: written, mode: 'status-only' })
  );
  log(`reve status: wrote ${written} ES stations`);
  return { upserted: written, statuses: desired.size, dryRun: false };
}

module.exports = {
  name: SOURCE,
  country: COUNTRY,
  // Area sweep (not an id-keyed bulk feed) — run.js dispatches to runAreaStatus.
  hasStatusFeed: true,
  fetch,
  // Resumable full crawl (REVE /locations is rate-limited below one-run coverage):
  // run.js drives this instead of fetch() when present. See crawlStatic.
  crawlStatic,
  clearCrawlState,
  fetchEnrichment,
  normalize,
  normalizeStreaming,
  parseStatus,
  runAreaStatus,
  // exported for unit tests
  locationToCharger,
  mapStandard,
  reveTariffToOcpi,
  openStateFromLocation,
  reveOcpiToCanonical,
  nearestMarker,
};
