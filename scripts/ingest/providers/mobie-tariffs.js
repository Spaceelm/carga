/**
 * MOBI.E per-plug operator fees (OPC), from MOBI.E's open data.
 *
 * Replaces the "Descarregar Tarifas" CSV (www.mobie.pt/documents/42032/106470/8602.csv).
 * That link started redirecting to an HTML page that still answers 200, so the CSV
 * parser found no columns and every station silently lost its fee: the app then priced
 * every CEME card with a national guess. Two rules follow, and are tested:
 *
 *   1. Fail loudly. A download that is not the expected JSON, or a run that attaches a
 *      fee to too few plugs, is a GitHub error annotation and a failed job — the
 *      charger data is still written, so the map never goes stale over a fee problem.
 *   2. Keep the whole rule. A fee is often conditional on how long the car stays:
 *      "€0.25/kWh until 60 min, then €0.20/min after 60 min". The old parser kept the
 *      first number and dropped the condition. Each plug now carries every component of
 *      its REGULAR plan (what a CEME-card user pays) with its time window, and the app
 *      prices the window against the simulated session for that plug and car.
 *
 * Source: https://data.mobie.pt/data/tariffs.json
 *   { source: {system, fetchedAt},
 *     sets: [[{ plan: 'regular'|'adhoc', price, unit: 'kWh'|'min'|'charge',
 *               after: minutes|null, until: minutes|null }]],
 *     stations: { 'LSB-00538': { '1': setIndex, '2': setIndex } } }
 * `after`/`until` are minutes since the session started.
 */

const TARIFFS_URL = 'https://data.mobie.pt/data/tariffs.json';
const MIN_STATIONS = 3000; // ~7,500 in Nov 2026; far fewer means a broken file
const MIN_COVERAGE = 0.5; // ~0.7 of PT plugs match in Nov 2026

const UNIT = { kWh: 'kWh', kwh: 'kWh', min: 'min', charge: 'charge' };

/** Report a fee problem the way GitHub Actions shows it, and fail the job at exit. */
function feeError(message) {
  console.error(`::error title=MOBI.E fees::${message}`);
  process.exitCode = 1;
}

const num = (v) => (v == null || v === '' ? null : Number.isFinite(Number(v)) ? Number(v) : NaN);

/**
 * Validate and index tariffs.json. Throws on a file that is not what we expect, so
 * the caller can report it instead of carrying on with nothing.
 * @returns {{ sets: Array, stations: Object, fetchedAt: string|null }}
 */
function parseTariffs(json) {
  const j = typeof json === 'string' ? JSON.parse(json) : json;
  if (!j || !Array.isArray(j.sets) || !j.stations || typeof j.stations !== 'object') {
    throw new Error('tariffs.json has no sets/stations');
  }
  const count = Object.keys(j.stations).length;
  if (count < MIN_STATIONS) throw new Error(`tariffs.json lists only ${count} stations`);
  return { sets: j.sets, stations: j.stations, fetchedAt: j.source?.fetchedAt ?? null };
}

/**
 * The MOBI.E station and plug a connector is, from its EVSE id, else its point id.
 *   PT*GLP*ELSB*00538*01  PT*EPK*ELSB00736*1  PT*FCT*E*VNG*00173*01  ->  LSB-00538 / 1
 *   LSB-00538-01                                                     ->  LSB-00538 / 1
 */
function plugKey(evseId, pointId) {
  const e = /^PT\*?[A-Z0-9]{3}\*?E\*?([A-Z]{3})\*?-?(\d{5})\*(\d+)$/i.exec(String(evseId || '').trim());
  if (e) return { station: `${e[1].toUpperCase()}-${e[2]}`, plug: String(Number(e[3])) };
  const p = /^([A-Z]{3}-\d{5})-(\d+)$/i.exec(String(pointId || '').trim());
  if (p) return { station: p[1].toUpperCase(), plug: String(Number(p[2])) };
  return null;
}

/**
 * A plug's set index. Most stations number plugs 1..n; some number them globally
 * (539, 540, 541 for plugs 01..03). Only then is the n-th plug the n-th key — never
 * when the station uses ordinal keys, where a missing number means a missing plug.
 */
function setIndexFor(stationPlugs, plug) {
  if (!stationPlugs) return null;
  if (stationPlugs[plug] != null) return stationPlugs[plug];
  const keys = Object.keys(stationPlugs).map(Number).filter(Number.isFinite).sort((a, b) => a - b);
  const ordinal = keys.some((k) => k <= keys.length);
  if (ordinal) return null;
  const n = Number(plug);
  return n >= 1 && n <= keys.length ? stationPlugs[String(keys[n - 1])] : null;
}

/**
 * The connector `opc` for a set: REGULAR plan only.
 *   energy/time/flat  the components that apply for the whole session (kept so an app
 *                     that does not know `parts` still prices the unconditional fee)
 *   parts             every component with its window, present only when some
 *                     component has one — the app prices these exactly
 * A plan with no regular components is a known zero fee, not an unknown one.
 */
function opcFromSet(set) {
  const regular = (set || []).filter((c) => c && c.plan === 'regular');
  const out = { energy: 0, time: 0, flat: 0 };
  const parts = [];
  let windowed = false;
  for (const c of regular) {
    const unit = UNIT[c.unit];
    const price = num(c.price);
    const after = num(c.after);
    const until = num(c.until);
    if (!unit || !(price >= 0) || Number.isNaN(after) || Number.isNaN(until)) continue;
    const always = (after == null || after === 0) && until == null;
    if (always) {
      if (unit === 'kWh') out.energy += price;
      else if (unit === 'min') out.time += price;
      else out.flat += price;
    } else {
      windowed = true;
    }
    parts.push({ unit, price, ...(after ? { after } : {}), ...(until != null ? { until } : {}) });
  }
  const r4 = (v) => Math.round(v * 1e5) / 1e5;
  const opc = { energy: r4(out.energy), time: r4(out.time), flat: r4(out.flat) };
  if (windowed) opc.parts = parts;
  return opc;
}

/** The `opc` for one connector, or null when MOBI.E publishes none for it. */
function opcForPlug(index, connector) {
  if (!index) return null;
  const k = plugKey(connector.evseId, connector.pointId);
  if (!k) return null;
  const i = setIndexFor(index.stations[k.station], k.plug);
  if (i == null || !index.sets[i]) return null;
  return opcFromSet(index.sets[i]);
}

/** Download and index; on any problem, report it loudly and return null. */
async function fetchTariffs(nodeFetch, userAgent) {
  try {
    const res = await nodeFetch(TARIFFS_URL, { headers: { 'User-Agent': userAgent, Accept: 'application/json' } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const type = res.headers.get('content-type') || '';
    if (!/json/i.test(type)) throw new Error(`expected JSON, got ${type || 'no content type'}`);
    return parseTariffs(await res.text());
  } catch (e) {
    feeError(`fee download failed (${e.message}); stations are written WITHOUT operator fees`);
    return null;
  }
}

/** Counts plugs priced vs seen in one run, and fails the job when too few match. */
function coverageTracker() {
  let seen = 0;
  let priced = 0;
  return {
    count(connectors) {
      for (const c of connectors) { seen++; if (c.opc) priced++; }
    },
    report(index) {
      const share = seen ? priced / seen : 0;
      console.log(`mobie: operator fee on ${priced}/${seen} plugs (${Math.round(share * 100)}%)${index?.fetchedAt ? `, fees from ${index.fetchedAt}` : ''}`);
      if (index && seen >= 500 && share < MIN_COVERAGE) {
        feeError(`only ${Math.round(share * 100)}% of plugs matched a fee (expected ≥ ${MIN_COVERAGE * 100}%): plug ids or the file changed`);
      }
      return { seen, priced, share };
    },
  };
}

// ---------------------------------------------------------------------------------
// Network membership: is a station on the MOBI.E network?
//
// The DATEX feed lists every public charger in Portugal, including networks outside
// MOBI.E (Continente, Tesla, High Green Power...). A CEME card only works on the MOBI.E
// network, so the app must know which is which or it prices CEME cards at stations
// where no driver can use them. MOBI.E's own station list says so per station.
//   https://data.mobie.pt/data/stations.json
//   { stations: [{ name: 'LSB-00538', partyId, mobieNetwork: true,
//                  sockets: [{ evse: 'LSB-00538-01', ... }] }] }
// ---------------------------------------------------------------------------------

const STATIONS_URL = 'https://data.mobie.pt/data/stations.json';
const MIN_LISTED = 5000; // ~8,800 in Nov 2026

/** station key / socket id -> mobieNetwork. Throws on a file that is not the list. */
function parseStations(json) {
  const j = typeof json === 'string' ? JSON.parse(json) : json;
  const list = j && Array.isArray(j.stations) ? j.stations : null;
  if (!list || list.length < MIN_LISTED) throw new Error(`stations.json lists ${list ? list.length : 0} stations`);
  const byKey = new Map();
  for (const s of list) {
    if (typeof s.mobieNetwork !== 'boolean') continue;
    if (s.name) byKey.set(String(s.name).trim().toUpperCase(), s.mobieNetwork);
    for (const k of s.sockets || []) {
      const evse = String(k.evse || '').trim().toUpperCase();
      if (!evse) continue;
      byKey.set(evse, s.mobieNetwork);
      const station = evse.replace(/-\d+$/, '');
      if (station !== evse && !byKey.has(station)) byKey.set(station, s.mobieNetwork);
    }
  }
  return { byKey, listed: list.length };
}

/**
 * true / false when MOBI.E lists the site, null when it does not (unknown: the app
 * then keeps pricing as before rather than hide a price on a guess). Tried in order:
 * each plug's MOBI.E station (from its EVSE id), its point id, then the site id
 * without its operator prefix ("CNT-A0104PL0001" -> "A0104PL0001").
 */
function networkForSite(index, site, connectors) {
  if (!index) return null;
  const get = (k) => (k ? index.byKey.get(String(k).trim().toUpperCase()) : undefined);
  for (const c of connectors || []) {
    const pk = plugKey(c.evseId, c.pointId);
    for (const v of [pk && get(pk.station), get(c.pointId)]) if (typeof v === 'boolean') return v;
  }
  for (const id of [site.id, site.externalIdentifier]) {
    if (!id) continue;
    for (const v of [get(id), get(String(id).replace(/^[A-Z0-9]{3}-/i, ''))]) if (typeof v === 'boolean') return v;
  }
  return null;
}

async function fetchStations(nodeFetch, userAgent) {
  try {
    const res = await nodeFetch(STATIONS_URL, { headers: { 'User-Agent': userAgent, Accept: 'application/json' } });
    if (!res.ok) throw new Error(`HTTP ${res.status}`);
    const type = res.headers.get('content-type') || '';
    if (!/json/i.test(type)) throw new Error(`expected JSON, got ${type || 'no content type'}`);
    return parseStations(await res.text());
  } catch (e) {
    feeError(`station list download failed (${e.message}); stations are written WITHOUT the MOBI.E network flag`);
    return null;
  }
}

module.exports = {
  TARIFFS_URL, MIN_COVERAGE, parseTariffs, plugKey, setIndexFor, opcFromSet, opcForPlug, fetchTariffs, coverageTracker,
  STATIONS_URL, parseStations, networkForSite, fetchStations,
};
