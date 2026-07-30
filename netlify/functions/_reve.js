/**
 * _reve.js — REVE (mapareve.es) live-status client for the Spanish charger overlay.
 *
 * REVE (Red de Recarga / SGV, run under Red Eléctrica; BOE-A-2025-7025) is Spain's
 * official national real-time EV-charging platform, published for third-party reuse.
 * Our own Spanish data comes from the DGT static feed, where every EVSE status is
 * UNKNOWN — so ES stations render as misleading grey "0". This module fetches REVE's
 * live marker status for a bbox at request time so the serving function can overlay a
 * REAL status onto each ES station (see mobie-charger-location.js).
 *
 * Everything here is defensive: on ANY error / non-200 / bad payload we return [] and
 * never throw. The serving function treats [] as "no live data this request" and falls
 * back to an operational default (see its ES overlay). We also cache the parsed marker
 * array in Upstash (short TTL) so panning the map doesn't hammer REVE — be a good citizen.
 *
 * CommonJS, uses node-fetch (already a dependency). No new deps, no secrets.
 */

const fetch = require('node-fetch');

const REVE_URL = 'https://www.mapareve.es/api/public/v1/markers';
const REVE_TIMEOUT_MS = 8000;
const REVE_CACHE_TTL_S = 60; // short: markers change; also throttles our load on REVE.

// Browser-like headers — REVE sits behind Incapsula; a plain fetch UA gets challenged.
// (Confirmed reachable server-side from Netlify with these exact headers.)
const REVE_HEADERS = {
  'User-Agent':
    'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/18.5 Safari/605.1.15',
  Accept: 'application/json',
  'Content-Type': 'application/json',
  Referer: 'https://www.mapareve.es/',
  Origin: 'https://www.mapareve.es',
};

// REVE clusters markers by zoom and returns HTTP 400 when a (zoom,bbox) pair would
// yield too many markers (empirically ~z16 is safe up to a ~13 km box ≈230 markers,
// ~z15 up to ~30 km). A fixed zoom therefore either over-clusters small views (few
// markers → almost nothing matches our per-station DGT data) or 400s on big ones.
// So we derive the zoom from the bbox span, scaling like a slippy map (zoom ~
// log2(1/span)) with a constant tuned to stay unclustered-but-under-cap.
const REVE_ZOOM_CONST = 6000;
const REVE_ZOOM_MIN = 8;
const REVE_ZOOM_MAX = 16;
// Above this span (degrees) REVE can only return heavily-clustered markers that won't
// align to individual stations, so we skip the fetch entirely (stations stay grey and
// get corrected once the user zooms into a viewport-sized area). ~1.5° ≈ 165 km.
const REVE_MAX_SPAN_DEG = 1.5;

/**
 * Pick a REVE zoom for a bbox so markers come back individual (not clustered) yet
 * under REVE's per-request cap. Pure function — unit-tested.
 * @param {{minLat:number,maxLat:number,minLon:number,maxLon:number}} bbox
 * @returns {number} integer zoom in [REVE_ZOOM_MIN, REVE_ZOOM_MAX]
 */
function reveZoomForBbox(bbox) {
  const latSpan = Math.abs(bbox.maxLat - bbox.minLat);
  const lonSpan = Math.abs(bbox.maxLon - bbox.minLon);
  const span = Math.max(latSpan, lonSpan) || 0.001;
  const z = Math.round(Math.log2(REVE_ZOOM_CONST / span));
  return Math.max(REVE_ZOOM_MIN, Math.min(REVE_ZOOM_MAX, z));
}

/**
 * Build the REVE /markers request body from a bbox. REVE expects the NE (max) and
 * SW (min) corners plus a zoom hint (derived from the span). Pure function — unit-tested.
 * @param {{minLat:number,maxLat:number,minLon:number,maxLon:number}} bbox
 * @returns {{latitude_ne:number,longitude_ne:number,latitude_sw:number,longitude_sw:number,zoom:number}}
 */
function bboxToReveBody(bbox) {
  return {
    latitude_ne: bbox.maxLat,
    longitude_ne: bbox.maxLon,
    latitude_sw: bbox.minLat,
    longitude_sw: bbox.minLon,
    zoom: reveZoomForBbox(bbox),
  };
}

/**
 * Cache key for a bbox: coords rounded to ~3 decimals (~100 m) so nearby pans reuse a
 * cached response instead of re-hitting REVE. Pure function — unit-tested.
 * @param {{minLat:number,maxLat:number,minLon:number,maxLon:number}} bbox
 * @returns {string}
 */
function reveCacheKey(bbox) {
  const r = (n) => Number(n).toFixed(3);
  return `reve:markers:${r(bbox.minLat)},${r(bbox.minLon)},${r(bbox.maxLat)},${r(bbox.maxLon)}`;
}

function num(v) {
  if (v == null) return null;
  const n = typeof v === 'number' ? v : parseFloat(v);
  return Number.isFinite(n) ? n : null;
}

/**
 * Normalize the raw REVE marker array into the compact shape the overlay needs.
 * Each marker carries a nested `.location` with the authoritative status/coords.
 * Skips entries without usable coordinates. Pure function — unit-tested.
 * @param {Array<any>} markers
 * @returns {Array<{lat:number,lon:number,status:string,totalEvse:number}>}
 */
function normalizeReveMarkers(markers) {
  if (!Array.isArray(markers)) return [];
  const out = [];
  for (const m of markers) {
    if (!m || typeof m !== 'object') continue;
    const loc = m.location || {};
    // Prefer the location's own coords; fall back to the marker's top-level coords.
    const lat = num(loc.latitude != null ? loc.latitude : m.latitude);
    const lon = num(loc.longitude != null ? loc.longitude : m.longitude);
    if (lat == null || lon == null) continue;
    out.push({
      lat,
      lon,
      status:
        typeof loc.status === 'string'
          ? loc.status
          : loc.status == null
          ? 'UNKNOWN'
          : String(loc.status),
      totalEvse: num(loc.total_evse != null ? loc.total_evse : m.total_evse) || 0,
    });
  }
  return out;
}

/**
 * Fetch live REVE marker statuses for a bbox.
 *
 * @param {{minLat:number,maxLat:number,minLon:number,maxLon:number}} bbox
 * @param {object} [opts]
 * @param {object} [opts.redis]  Optional Upstash client (the serving fn's). If given,
 *   results are read from / written to a short-TTL cache keyed by the rounded bbox.
 * @returns {Promise<Array<{lat:number,lon:number,status:string,totalEvse:number}>>}
 *   Never throws. Returns [] on any error / non-200 / bad payload.
 */
async function fetchReveStatus(bbox, opts = {}) {
  const { redis } = opts;

  if (
    !bbox ||
    !Number.isFinite(bbox.minLat) || !Number.isFinite(bbox.maxLat) ||
    !Number.isFinite(bbox.minLon) || !Number.isFinite(bbox.maxLon)
  ) {
    return [];
  }

  // Oversized bbox (initial-load bulk tiles): REVE can only cluster at that scale, so
  // skip the fetch — cheaper than 25 near-useless calls, and the area gets a real
  // overlay once the user zooms into a viewport-sized view.
  const span = Math.max(Math.abs(bbox.maxLat - bbox.minLat), Math.abs(bbox.maxLon - bbox.minLon));
  if (span > REVE_MAX_SPAN_DEG) return [];

  const cacheKey = reveCacheKey(bbox);

  // 1) Cache read (best-effort; a cache miss/error must not block the live fetch).
  if (redis) {
    try {
      const cached = await redis.get(cacheKey);
      if (cached != null) {
        const arr = typeof cached === 'string' ? JSON.parse(cached) : cached;
        if (Array.isArray(arr)) return arr;
      }
    } catch (err) {
      console.error('[reve] cache read failed:', err && (err.stack || err.message));
    }
  }

  // 2) Live fetch (timeout-guarded, never throws out of this function).
  let markers;
  try {
    const controller = new AbortController();
    const timeoutId = setTimeout(() => controller.abort(), REVE_TIMEOUT_MS);
    let res;
    try {
      res = await fetch(REVE_URL, {
        method: 'POST',
        headers: REVE_HEADERS,
        body: JSON.stringify(bboxToReveBody(bbox)),
        signal: controller.signal,
      });
    } finally {
      clearTimeout(timeoutId);
    }
    if (!res || !res.ok) {
      console.error('[reve] non-200 response:', res && res.status);
      return [];
    }
    const raw = await res.json();
    markers = normalizeReveMarkers(raw);
  } catch (err) {
    console.error('[reve] fetch failed:', err && (err.stack || err.message));
    return [];
  }

  // 3) Cache write (best-effort).
  if (redis) {
    try {
      await redis.set(cacheKey, JSON.stringify(markers), { ex: REVE_CACHE_TTL_S });
    } catch (err) {
      console.error('[reve] cache write failed:', err && (err.stack || err.message));
    }
  }

  return markers;
}

/**
 * Map a REVE `location.status` onto the per-EVSE OCPI status token the frontend uses.
 * The frontend (01-group_similar_stations.js) counts ONLY status === "AVAILABLE" as
 * available; every other token renders as not-available but with a REAL reason.
 *
 *   AVAILABLE                                   -> AVAILABLE   (counts as available)
 *   CHARGING                                    -> CHARGING    (in use, not available)
 *   OUTOFORDER                                  -> OUTOFORDER  (broken, not available)
 *   OCCUPIED | RESERVED | BLOCKED | INOPERATIVE -> UNAVAILABLE (not available)
 *   UNKNOWN | null | anything else              -> null (caller applies operational default)
 *
 * Returns null for unknown/unmapped values so the caller can apply its "assume
 * operational" default instead of surfacing a misleading unknown state.
 * Pure function — unit-tested.
 * @param {string} reveStatus
 * @returns {string|null}
 */
function reveStatusToOcpi(reveStatus) {
  switch (String(reveStatus || '').toUpperCase()) {
    case 'AVAILABLE':
      return 'AVAILABLE';
    case 'CHARGING':
      return 'CHARGING';
    case 'OUTOFORDER':
      return 'OUTOFORDER';
    case 'OCCUPIED':
    case 'RESERVED':
    case 'BLOCKED':
    case 'INOPERATIVE':
      return 'UNAVAILABLE';
    case 'UNKNOWN':
    case '':
      return null;
    default:
      return null;
  }
}

module.exports = {
  fetchReveStatus,
  reveStatusToOcpi,
  bboxToReveBody,
  reveZoomForBbox,
  reveCacheKey,
  normalizeReveMarkers,
  REVE_URL,
  REVE_HEADERS,
};
