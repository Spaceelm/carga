/**
 * providers/mobie.js
 *
 * MOBI.E NAP provider (Portugal). Reads two DATEX II v3 XML feeds:
 *   - static  (evChargingInfra):  locations / specs / tariffs   (~183 MB)
 *   - status  (evActualStatus):   live availability per point    (~40 MB)
 *
 * Both are parsed with a STREAMING SAX parser so the 183 MB static feed never lives
 * in memory as a single string/DOM. We walk it site-by-site and emit normalized
 * records into the common schema, joining live status by refill-point id.
 *
 * Static structure (namespaces stripped — we match on localName):
 *   energyInfrastructureSite (@id)
 *     name > values > value                                (site display name)
 *     externalIdentifier                                   (site external id)
 *     lastUpdated
 *     locationReference
 *       facilityLocation > address (postcode, city>values>value, addressLine>text>values>value)
 *       pointByCoordinates > pointCoordinates (latitude, longitude)
 *     operator (@id) > name>values>value                   (operator display name)
 *     energyInfrastructureStation
 *       refillPoint (@id)                                   (== join key to status)
 *         externalIdentifier                               (e.g. PT*EZC*E*AMD*00051*1)
 *         connector (connectorType, connectorFormat, maxPowerAtSocket[W], voltage, maximumCurrent)
 *
 * Status structure:
 *   energyInfrastructureSiteStatus
 *     ... refillPointStatus > reference@id (== refillPoint @id) + status
 */

const sax = require('sax');
const { CHARGER_STATUS, CONNECTOR_STANDARD } = require('../schema');

const STATIC_URL = 'https://pgm.mobie.pt/integration/nap/evChargingInfra';
const STATUS_URL = 'https://pgm.mobie.pt/integration/nap/evActualStatus';
// Per-plug operator fees (OPC) come from MOBI.E's open data: see ./mobie-tariffs.js.
const USER_AGENT = 'carga-live-web/1.0';
const COUNTRY = 'PT';
const SOURCE = 'mobie';
const { fetchTariffs, opcForPlug, coverageTracker, fetchStations, networkForSite } = require('./mobie-tariffs');
// Fee index for the current full run (null on a status-only run or a failed download),
// and the plugs-priced counter it reports. Module-level so run.js keeps its signature.
let feeIndex = null;
// MOBI.E's own station list: which sites are on the MOBI.E network (CEME cards work
// only there). null on a status-only run or a failed download.
let networkIndex = null;
const networkTally = { mobie: 0, other: 0, unknown: 0 };
let feeCoverage = null;

/** Strip an XML namespace prefix ("ns6:foo" -> "foo"). */
function localName(qname) {
  const i = qname.indexOf(':');
  return i === -1 ? qname : qname.slice(i + 1);
}

/**
 * Map a MOBI.E connectorType token to a canonical connector standard.
 * @param {string} raw e.g. "iec62196T2", "iec62196T2COMBO", "chademo"
 */
function mapConnectorType(raw) {
  if (!raw) return CONNECTOR_STANDARD.UNKNOWN;
  const t = String(raw).toLowerCase();
  if (t.includes('combo') || t.includes('ccs')) return CONNECTOR_STANDARD.CCS;
  if (t.includes('chademo')) return CONNECTOR_STANDARD.CHADEMO;
  if (t.includes('t2') || t === 'iec62196t2') return CONNECTOR_STANDARD.TYPE2;
  if (t.includes('t1')) return CONNECTOR_STANDARD.TYPE1;
  if (t.includes('t3')) return CONNECTOR_STANDARD.TYPE3;
  if (t.includes('tesla')) return CONNECTOR_STANDARD.TESLA;
  return CONNECTOR_STANDARD.UNKNOWN;
}

/**
 * Map a MOBI.E status enum to the canonical ChargerStatus.
 * MOBI.E: available/charging/outOfOrder/planned/removed/unknown/inoperative/blocked
 */
function mapStatus(raw) {
  if (!raw) return CHARGER_STATUS.UNKNOWN;
  switch (String(raw).trim()) {
    case 'available': return CHARGER_STATUS.AVAILABLE;
    case 'charging': return CHARGER_STATUS.CHARGING;
    case 'planned': return CHARGER_STATUS.PLANNED;
    case 'removed': return CHARGER_STATUS.REMOVED;
    case 'outOfOrder':
    case 'inoperative':
    case 'blocked': return CHARGER_STATUS.OUT_OF_ORDER;
    default: return CHARGER_STATUS.UNKNOWN;
  }
}

/** Normalize maxPowerAtSocket (Watts) → kW, or null. */
function wattsToKW(w) {
  const n = parseFloat(w);
  if (!isFinite(n) || n <= 0) return null;
  return Math.round((n / 1000) * 10) / 10;
}

function numOrNull(v) {
  const n = parseFloat(v);
  return isFinite(n) ? n : null;
}

/** Pipe a Readable stream or a string into a sax stream. */
function pipeInput(input, parser, reject) {
  if (typeof input === 'string') {
    parser.write(input);
    parser.end();
  } else if (input && typeof input.pipe === 'function') {
    input.on('error', reject);
    input.pipe(parser);
  } else {
    reject(new Error('mobie: unsupported input (expected Readable stream or string)'));
  }
}

/**
 * Stream-parse the STATUS feed into a Map<pointId, {status, lastUpdated}>.
 * ~40 MB → the resulting map (~20k small entries) is fine to hold in memory.
 *
 * @param {import('stream').Readable|string} input
 * @returns {Promise<Map<string,{status:string,lastUpdated:string|null}>>}
 */
function parseStatus(input) {
  return new Promise((resolve, reject) => {
    const statusById = new Map();
    const parser = sax.createStream(true, { trim: true, position: false });

    let inRefillPointStatus = false;
    let currentPointId = null;
    let capturingStatus = false;
    let statusText = '';

    parser.on('opentag', (node) => {
      const name = localName(node.name);
      if (name === 'refillPointStatus') {
        inRefillPointStatus = true;
        currentPointId = null;
      } else if (name === 'reference' && inRefillPointStatus && currentPointId === null) {
        currentPointId = node.attributes.id || node.attributes.ID || null;
      } else if (name === 'status' && inRefillPointStatus) {
        capturingStatus = true;
        statusText = '';
      }
    });

    parser.on('text', (t) => { if (capturingStatus) statusText += t; });
    parser.on('cdata', (t) => { if (capturingStatus) statusText += t; });

    parser.on('closetag', (tag) => {
      const name = localName(tag);
      if (name === 'status' && capturingStatus) {
        capturingStatus = false;
        if (currentPointId) {
          statusById.set(currentPointId, { status: mapStatus(statusText), lastUpdated: null });
        }
      } else if (name === 'refillPointStatus') {
        inRefillPointStatus = false;
        currentPointId = null;
      }
    });

    parser.on('error', reject);
    parser.on('end', () => resolve(statusById));

    pipeInput(input, parser, reject);
  });
}

/**
 * Finalize a per-site accumulator into a common-schema Charger, or null if unusable.
 */
// MOBI.E lists ~5% of refill points twice — verbatim, or under two ID conventions
// that differ only by '*' delimiters (e.g. PT*ATL*EARR*80001*1 vs PT*ATL*E*ARR*80001*1).
// Both are the same physical plug. Collapse them: key by the ID with '*' stripped,
// keep the most-useful status (a free plug beats a busy/broken/unknown twin).
const DEDUP_STATUS_RANK = {
  [CHARGER_STATUS.AVAILABLE]: 4,
  [CHARGER_STATUS.CHARGING]: 3,
  [CHARGER_STATUS.OUT_OF_ORDER]: 2,
  [CHARGER_STATUS.PLANNED]: 1,
  [CHARGER_STATUS.REMOVED]: 0,
  [CHARGER_STATUS.UNKNOWN]: 0,
};
function dedupeConnectors(connectors) {
  const byKey = new Map();
  let noId = 0;
  for (const c of connectors || []) {
    const canon = String(c.evseId || c.pointId || '').replace(/\*/g, '').toUpperCase();
    const key = canon || `__noid_${noId++}`;
    const prev = byKey.get(key);
    if (!prev) { byKey.set(key, c); continue; }
    const rank = (st) => (DEDUP_STATUS_RANK[st] != null ? DEDUP_STATUS_RANK[st] : 1);
    if (rank(c.status) > rank(prev.status)) byKey.set(key, c);
  }
  return [...byKey.values()];
}

// DATEX II authenticationAndIdentificationMethods -> our OCPI-style capability
// tokens (same vocabulary REVE uses), so PT feeds the capability filters. Keys are
// normalized to lowercase letters only ("creditCard"/"credit_card" -> "creditcard").
const AUTH_CAP_MAP = {
  creditcard: 'CREDIT_CARD_PAYABLE',
  debitcard: 'DEBIT_CARD_PAYABLE',
  contactless: 'CONTACTLESS_CARD_SUPPORT',
  nfc: 'CONTACTLESS_CARD_SUPPORT',
  rfid: 'RFID_READER',
  pinpad: 'PED_TERMINAL',
  // Non-card auth methods. Mapped so a site that advertises ONLY these still yields
  // a non-empty capabilities list — that lets consumers tell "has payment/auth info
  // but no card" apart from "no info at all" (e.g. the Recommended card rule keeps
  // the latter but drops the former). app/web-initiated sessions ~= remote start.
  apps: 'REMOTE_START_STOP_CAPABLE',
  website: 'REMOTE_START_STOP_CAPABLE',
};

/** Map a site's DATEX auth/identification methods to deduped capability tokens. */
function authMethodsToCapabilities(methods) {
  const caps = new Set();
  for (const m of methods || []) {
    const key = String(m).toLowerCase().replace(/[^a-z]/g, '');
    if (AUTH_CAP_MAP[key]) caps.add(AUTH_CAP_MAP[key]);
  }
  return [...caps];
}

function finalizeSite(site) {
  if (site.lat === null || site.lon === null) return null;
  if (!isFinite(site.lat) || !isFinite(site.lon)) return null;

  const connectors = dedupeConnectors(site.connectors);
  const aggregate = aggregateStatus(connectors.map((c) => c.status));
  const capabilities = authMethodsToCapabilities(site.authMethods);

  // Attach each plug's operator fee from MOBI.E's open data, matched by its EVSE id.
  // A plug MOBI.E publishes no fee for gets none; the app then estimates one.
  if (feeIndex) {
    for (const c of connectors) {
      const opc = opcForPlug(feeIndex, c);
      if (opc) c.opc = opc;
    }
    feeCoverage.count(connectors);
  }
  const mobieNetwork = networkForSite(networkIndex, site, connectors);
  if (networkIndex) networkTally[mobieNetwork === true ? 'mobie' : mobieNetwork === false ? 'other' : 'unknown']++;
  return {
    id: site.id || site.externalIdentifier || `${site.lat}_${site.lon}`,
    country: COUNTRY,
    source: SOURCE,
    lat: site.lat,
    lon: site.lon,
    name: site.name || site.externalIdentifier || 'Charging Station',
    address: site.address || '',
    city: site.city || '',
    postcode: site.postcode || '',
    operator: site.operator || '',
    open24h: site.open24h != null ? site.open24h : null,
    capabilities,
    connectors,
    tariff: null,
    // true: on the MOBI.E network; false: listed by MOBI.E as outside it (Continente,
    // Tesla...); absent: not known. The app prices CEME cards only where it is true or absent.
    ...(mobieNetwork != null ? { mobieNetwork } : {}),
    status: aggregate,
    lastUpdated: site.lastUpdated || new Date().toISOString(),
  };
}

/** Pick a representative site status from its connectors. */
function aggregateStatus(statuses) {
  if (!statuses.length) return CHARGER_STATUS.UNKNOWN;
  if (statuses.includes(CHARGER_STATUS.AVAILABLE)) return CHARGER_STATUS.AVAILABLE;
  if (statuses.includes(CHARGER_STATUS.CHARGING)) return CHARGER_STATUS.CHARGING;
  if (statuses.every((s) => s === CHARGER_STATUS.OUT_OF_ORDER)) return CHARGER_STATUS.OUT_OF_ORDER;
  if (statuses.every((s) => s === CHARGER_STATUS.PLANNED)) return CHARGER_STATUS.PLANNED;
  if (statuses.every((s) => s === CHARGER_STATUS.REMOVED)) return CHARGER_STATUS.REMOVED;
  return statuses.find((s) => s !== CHARGER_STATUS.UNKNOWN) || CHARGER_STATUS.UNKNOWN;
}

/**
 * Fetch both feeds and return their response bodies as Readable streams.
 * @param {{statusOnly?: boolean}} [opts]
 */
async function fetch(opts = {}) {
  const nodeFetch = require('node-fetch');
  const headers = { 'User-Agent': USER_AGENT, Accept: 'application/xml, text/xml, */*' };

  const statusRes = await nodeFetch(STATUS_URL, { headers });
  if (!statusRes.ok) {
    throw new Error(`mobie: status feed HTTP ${statusRes.status} ${statusRes.statusText}`);
  }
  const statusStream = statusRes.body;

  let staticStream = null;
  if (!opts.statusOnly) {
    const staticRes = await nodeFetch(STATIC_URL, { headers });
    if (!staticRes.ok) {
      throw new Error(`mobie: static feed HTTP ${staticRes.status} ${staticRes.statusText}`);
    }
    staticStream = staticRes.body;
    // Per-plug operator fees. A failed download is reported (job fails) but the
    // charger data is still written: stations must not go stale over a fee problem.
    feeIndex = await fetchTariffs(nodeFetch, USER_AGENT);
    feeCoverage = coverageTracker();
    networkIndex = await fetchStations(nodeFetch, USER_AGENT);
  }

  return { staticStream, statusStream };
}

/**
 * Normalize the two feeds into an array of common-schema Chargers.
 * Accepts XML strings (tests) or Readable streams (production).
 * @param {import('stream').Readable|string} staticInput
 * @param {import('stream').Readable|string} statusInput
 * @returns {Promise<import('../schema').Charger[]>}
 */
async function normalize(staticInput, statusInput) {
  const chargers = [];
  await normalizeStreaming(staticInput, statusInput, (c) => chargers.push(c));
  return chargers;
}

/**
 * Streaming variant: invokes onCharger per record instead of buffering.
 * @param {import('stream').Readable|string} staticInput
 * @param {import('stream').Readable|string} statusInput
 * @param {(c:import('../schema').Charger)=>(void|Promise<void>)} onCharger
 * @returns {Promise<{sites:number, points:number, statusCount:number}>}
 */
async function normalizeStreaming(staticInput, statusInput, onCharger) {
  const statusById = statusInput ? await parseStatus(statusInput) : new Map();

  const { sites, points } = await new Promise((resolve, reject) => {
    const parser = sax.createStream(true, { trim: true, position: false });
    buildStaticState(statusById, onCharger, parser, resolve, reject);
    pipeInput(staticInput, parser, reject);
  });

  return { sites, points, statusCount: statusById.size };
}

/**
 * Wire the static-feed handlers onto a sax stream. Kept as one function so the
 * parser handle is available to attach the input (fixes the earlier resolve gap).
 */
function buildStaticState(statusById, onCharger, parser, resolve, reject) {
  let sites = 0;
  let points = 0;

  let site = null;
  let point = null;
  let connector = null;

  let inOperator = false;
  let inAddress = false;
  let inCity = false;
  let inCoordinates = false;
  let nameDepth = 0;

  let capture = null;
  const startCapture = (key) => { capture = { key, text: '' }; };

  function applyCapture(key, rawText) {
    const text = rawText.trim();
    if (!text) return;
    switch (key) {
      case 'site.name': if (site && site.name === null) site.name = text; break;
      case 'site.externalIdentifier': if (site) site.externalIdentifier = text; break;
      case 'site.lastUpdated': if (site) site.lastUpdated = text; break;
      case 'point.externalIdentifier': if (point) point.evseId = text; break;
      case 'lat': if (site) site.lat = numOrNull(text); break;
      case 'lon': if (site) site.lon = numOrNull(text); break;
      case 'postcode': if (site && site.postcode === null) site.postcode = text; break;
      case 'addressLine': if (site && site.address === null) site.address = text; break;
      case 'city': if (site && site.city === null) site.city = text; break;
      case 'operator.name': if (site && site.operator === null) site.operator = text; break;
      case 'connector.type': if (connector) connector.type = text; break;
      case 'connector.format': if (connector) connector.format = text; break;
      case 'connector.chargingMode': if (connector) connector.chargingMode = text; break;
      case 'auth': if (site) site.authMethods.push(text); break;
      case 'connector.power': if (connector) connector.power = text; break;
      case 'connector.voltage': if (connector) connector.voltage = text; break;
      case 'connector.amperage': if (connector) connector.amperage = text; break;
      default: break;
    }
  }

  parser.on('opentag', (node) => {
    const name = localName(node.name);
    switch (name) {
      case 'energyInfrastructureSite':
        site = {
          id: node.attributes.id || node.attributes.ID || null,
          name: null, externalIdentifier: null, lastUpdated: null,
          lat: null, lon: null, address: null, city: null, postcode: null,
          operator: null, open24h: null, authMethods: [], connectors: [],
        };
        break;
      case 'operatingHours':
        // DATEX II site opening hours. OpenAllHours => 24/7; an
        // OperatingHoursSpecification (present but the feed omits a concrete weekly
        // schedule) => not 24/7 (limited / venue hours). Absent => unknown (null).
        if (site && !point) {
          const xt = node.attributes['xsi:type'] || node.attributes['XSI:TYPE'] || '';
          if (/OpenAllHours/i.test(xt)) site.open24h = true;
          else if (/OperatingHoursSpecification/i.test(xt)) site.open24h = false;
        }
        break;
      case 'operator': inOperator = true; break;
      case 'address': inAddress = true; break;
      case 'city': inCity = true; break;
      case 'name': nameDepth++; break;
      case 'pointCoordinates': inCoordinates = true; break;
      case 'refillPoint':
        point = {
          pointId: node.attributes.id || node.attributes.ID || null,
          evseId: null, standard: CONNECTOR_STANDARD.UNKNOWN, format: 'unknown',
          chargingMode: null, powerKW: null, voltage: null, amperage: null,
        };
        break;
      case 'connector': if (point) connector = {}; break;
      case 'externalIdentifier':
        if (point) startCapture('point.externalIdentifier');
        else if (site) startCapture('site.externalIdentifier');
        break;
      case 'lastUpdated': if (site && !point) startCapture('site.lastUpdated'); break;
      case 'latitude': if (inCoordinates) startCapture('lat'); break;
      case 'longitude': if (inCoordinates) startCapture('lon'); break;
      case 'postcode': if (inAddress) startCapture('postcode'); break;
      case 'connectorType': if (connector) startCapture('connector.type'); break;
      case 'connectorFormat': if (connector) startCapture('connector.format'); break;
      case 'chargingMode': if (connector) startCapture('connector.chargingMode'); break;
      case 'authenticationAndIdentificationMethods': if (site) startCapture('auth'); break;
      case 'maxPowerAtSocket': if (connector) startCapture('connector.power'); break;
      case 'voltage': if (connector) startCapture('connector.voltage'); break;
      case 'maximumCurrent': if (connector) startCapture('connector.amperage'); break;
      case 'text':
        if (inAddress && !inCity && site && site.address === null) startCapture('addressLine');
        break;
      case 'value':
        if (capture && capture.key === 'addressLine') break;
        if (inCity && site && site.city === null) startCapture('city');
        else if (inOperator && nameDepth > 0 && site && site.operator === null) startCapture('operator.name');
        else if (nameDepth > 0 && !inOperator && !inAddress && !inCity && site && site.name === null) startCapture('site.name');
        break;
      default: break;
    }
  });

  parser.on('text', (t) => { if (capture) capture.text += t; });
  parser.on('cdata', (t) => { if (capture) capture.text += t; });

  parser.on('closetag', (tag) => {
    const name = localName(tag);

    if (capture) {
      const owns =
        (name === 'value' && (capture.key === 'site.name' || capture.key === 'operator.name' || capture.key === 'city')) ||
        (name === 'externalIdentifier' && (capture.key === 'site.externalIdentifier' || capture.key === 'point.externalIdentifier')) ||
        (name === 'lastUpdated' && capture.key === 'site.lastUpdated') ||
        (name === 'latitude' && capture.key === 'lat') ||
        (name === 'longitude' && capture.key === 'lon') ||
        (name === 'postcode' && capture.key === 'postcode') ||
        (name === 'text' && capture.key === 'addressLine') ||
        (name === 'connectorType' && capture.key === 'connector.type') ||
        (name === 'connectorFormat' && capture.key === 'connector.format') ||
        (name === 'chargingMode' && capture.key === 'connector.chargingMode') ||
        (name === 'authenticationAndIdentificationMethods' && capture.key === 'auth') ||
        (name === 'maxPowerAtSocket' && capture.key === 'connector.power') ||
        (name === 'voltage' && capture.key === 'connector.voltage') ||
        (name === 'maximumCurrent' && capture.key === 'connector.amperage');
      if (owns) { applyCapture(capture.key, capture.text); capture = null; }
    }

    switch (name) {
      case 'operator': inOperator = false; break;
      case 'address': inAddress = false; break;
      case 'city': inCity = false; break;
      case 'name': if (nameDepth > 0) nameDepth--; break;
      case 'pointCoordinates': inCoordinates = false; break;
      case 'connector':
        if (point && connector) {
          point.standard = mapConnectorType(connector.type);
          point.format = connector.format || 'unknown';
          point.chargingMode = connector.chargingMode || null;
          point.powerKW = wattsToKW(connector.power);
          point.voltage = numOrNull(connector.voltage);
          point.amperage = numOrNull(connector.amperage);
        }
        connector = null;
        break;
      case 'refillPoint':
        if (site && point) {
          const live = point.pointId ? statusById.get(point.pointId) : null;
          site.connectors.push({
            standard: point.standard,
            format: point.format,
            chargingMode: point.chargingMode,
            powerKW: point.powerKW,
            voltage: point.voltage,
            amperage: point.amperage,
            pointId: point.pointId,
            evseId: point.evseId || point.pointId,
            status: live ? live.status : CHARGER_STATUS.UNKNOWN,
            lastUpdated: site.lastUpdated || null,
          });
          points++;
        }
        point = null;
        break;
      case 'energyInfrastructureSite':
        if (site) {
          const charger = finalizeSite(site);
          if (charger) { onCharger(charger); sites++; }
        }
        site = null;
        break;
      default: break;
    }
  });

  parser.on('error', reject);
  parser.on('end', () => {
    if (feeIndex && feeCoverage) feeCoverage.report(feeIndex);
    if (networkIndex) console.log(`mobie: network flag on ${networkTally.mobie} MOBI.E + ${networkTally.other} non-MOBI.E sites, ${networkTally.unknown} not listed`);
    resolve({ sites, points });
  });
}

module.exports = {
  name: SOURCE,
  country: COUNTRY,
  fetch,
  normalize,
  normalizeStreaming,
  parseStatus,
  // exported for unit tests
  _mapConnectorType: mapConnectorType,
  _mapStatus: mapStatus,
  _wattsToKW: wattsToKW,
};
