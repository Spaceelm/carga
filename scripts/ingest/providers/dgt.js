/**
 * providers/dgt.js
 *
 * DGT NAP provider (Spain). Reads ONE DATEX II v3 XML feed:
 *   - static (electrolineras.xml): locations / specs   (~85 MB, ~12k sites)
 *
 * There is NO live-status feed for Spain (unlike PT/MOBI.E). Every connector is
 * emitted with status "unknown"; the serving layer maps that to "UNKNOWN".
 *
 * The logical model is the SAME as MOBI.E, but the XML uses DIFFERENT namespace
 * prefixes (egi:/loc:/fac:/locx:/com: vs MOBI.E's ns4:/ns6:...). We parse purely
 * by LOCAL element name (prefix stripped) so the provider is namespace-agnostic.
 *
 * Structural differences from MOBI.E worth noting (see also finalizeSite/handlers):
 *   - Coordinates wrapper is `coordinatesForDisplay` (MOBI.E: `pointCoordinates`).
 *   - Stable ids live as ATTRIBUTES: energyInfrastructureSite@id (site) and
 *     refillPoint@id (point). We prefix the site id with "ES-" for the Redis key
 *     so it can never collide with a PT id.
 *   - A refillPoint has NO `externalIdentifier`; its EVSE id is the refillPoint's
 *     `name` (e.g. "ES*CRF*EBDJZVALVERDE").
 *   - A single refillPoint may hold MULTIPLE `connector` children (e.g. one point
 *     exposing both CCS and CHAdeMO). We emit one common-schema connector PER
 *     `<connector>` element (MOBI.E emitted one per refillPoint).
 *   - Address is a list of labeled `addressLine`s ("Dirección: ...", "Municipio:
 *     ...", "Provincia: ..."). We take "Dirección:" as the street and "Municipio:"
 *     as the city; some connectors carry no `voltage` element -> null.
 *
 * Static structure (namespaces stripped):
 *   energyInfrastructureSite (@id  <- stable site id)
 *     name > values > value                                 (site display name)
 *     lastUpdated
 *     locationReference
 *       facilityLocation > address (postcode, addressLine > text > values > value)
 *       coordinatesForDisplay (latitude, longitude)
 *     operator (@id) > name > values > value                (operator display name)
 *     energyInfrastructureStation
 *       refillPoint (@id  <- stable point id)
 *         name > values > value                             (EVSE id, e.g. ES*CRF*...)
 *         connector (connectorType, connectorFormat, maxPowerAtSocket[W], voltage, maximumCurrent)
 *         [connector ...]                                   (may repeat)
 */

const sax = require('sax');
const { CHARGER_STATUS } = require('../schema');
const {
  localName,
  mapConnectorType,
  wattsToKW,
  numOrNull,
  pipeInput,
} = require('./_shared');

const STATIC_URL =
  'https://infocar.dgt.es/datex2/v3/miterd/EnergyInfrastructureTablePublication/electrolineras.xml';
const USER_AGENT = 'carga-live-web/1.0';
const COUNTRY = 'ES';
const SOURCE = 'dgt';
const ID_PREFIX = 'ES-'; // keep ES ids in a separate namespace from PT ids

/**
 * Finalize a per-site accumulator into a common-schema Charger, or null if unusable.
 * Spain has no live status, so every connector/site is "unknown".
 */
function finalizeSite(site) {
  if (site.lat === null || site.lon === null) return null;
  if (!isFinite(site.lat) || !isFinite(site.lon)) return null;

  return {
    id: site.id,
    country: COUNTRY,
    source: SOURCE,
    lat: site.lat,
    lon: site.lon,
    name: site.name || site.address || 'Charging Station',
    address: site.address || '',
    city: site.city || '',
    postcode: site.postcode || '',
    operator: site.operator || '',
    connectors: site.connectors,
    tariff: null,
    status: CHARGER_STATUS.UNKNOWN, // no ES status feed
    lastUpdated: site.lastUpdated || new Date().toISOString(),
  };
}

/**
 * Fetch the static feed and return its response body as a Readable stream.
 * `statusOnly` is accepted for interface symmetry with mobie but is a no-op:
 * Spain has no status feed, so a status-only run yields nothing to fetch.
 * @param {{statusOnly?: boolean}} [opts]
 */
async function fetch(opts = {}) {
  if (opts.statusOnly) {
    // No live-status feed for Spain — nothing to fetch.
    return { staticStream: null, statusStream: null };
  }

  const nodeFetch = require('node-fetch');
  const headers = { 'User-Agent': USER_AGENT, Accept: 'application/xml, text/xml, */*' };

  const staticRes = await nodeFetch(STATIC_URL, { headers });
  if (!staticRes.ok) {
    throw new Error(`dgt: static feed HTTP ${staticRes.status} ${staticRes.statusText}`);
  }
  return { staticStream: staticRes.body, statusStream: null };
}

/**
 * Spain has no status feed. Provided for interface parity with mobie so run.js's
 * status-only path can call it uniformly; always resolves to an empty map.
 * @returns {Promise<Map<string,{status:string,lastUpdated:string|null}>>}
 */
async function parseStatus() {
  return new Map();
}

/**
 * Normalize the static feed into an array of common-schema Chargers.
 * Accepts an XML string (tests) or a Readable stream (production). The second
 * argument (statusInput) is ignored — kept for signature parity with mobie.
 * @param {import('stream').Readable|string} staticInput
 * @returns {Promise<import('../schema').Charger[]>}
 */
async function normalize(staticInput /*, statusInput */) {
  const chargers = [];
  await normalizeStreaming(staticInput, null, (c) => chargers.push(c));
  return chargers;
}

/**
 * Streaming variant: invokes onCharger per record instead of buffering.
 * @param {import('stream').Readable|string} staticInput
 * @param {*} _statusInput  ignored (no ES status feed)
 * @param {(c:import('../schema').Charger)=>(void|Promise<void>)} onCharger
 * @returns {Promise<{sites:number, points:number, statusCount:number}>}
 */
async function normalizeStreaming(staticInput, _statusInput, onCharger) {
  const { sites, points } = await new Promise((resolve, reject) => {
    const parser = sax.createStream(true, { trim: true, position: false });
    buildStaticState(onCharger, parser, resolve, reject);
    pipeInput(staticInput, parser, reject);
  });

  return { sites, points, statusCount: 0 };
}

/**
 * Wire the static-feed handlers onto a sax stream.
 *
 * Address handling: DGT emits several labeled `addressLine`s. Each is
 * `addressLine > text > values > value`, and the value string is prefixed with a
 * Spanish label ("Dirección: ", "Municipio: ", ...). We capture every addressLine
 * value and route it by its label in applyAddressLine().
 */
function buildStaticState(onCharger, parser, resolve, reject) {
  let sites = 0;
  let points = 0;

  let site = null;
  let point = null;       // { pointId, evseId, connectors:[] }
  let connector = null;   // raw { type, format, power, voltage, amperage }

  let inOperator = false;
  let inAddress = false;
  let inAddressLine = false;
  let inCoordinates = false;
  let nameDepth = 0;

  let capture = null;
  const startCapture = (key) => { capture = { key, text: '' }; };

  function applyAddressLine(rawText) {
    const text = rawText.trim();
    if (!text || !site) return;
    // Labeled lines: "Dirección: <street>", "Municipio: <city>".
    const m = text.match(/^([^:]+):\s*(.*)$/);
    const label = m ? m[1].trim().toLowerCase() : '';
    const val = m ? m[2].trim() : text;
    if (label.startsWith('direcci')) {              // Dirección
      if (site.address === null) site.address = val;
    } else if (label.startsWith('municipio')) {      // Municipio
      if (site.city === null) site.city = val;
    } else if (!m && site.address === null) {
      // Unlabeled free-text line -> treat as the street address.
      site.address = text;
    }
  }

  function applyCapture(key, rawText) {
    const text = rawText.trim();
    if (key === 'addressLine') { applyAddressLine(rawText); return; }
    if (!text) return;
    switch (key) {
      case 'site.name': if (site && site.name === null) site.name = text; break;
      case 'site.lastUpdated': if (site) site.lastUpdated = text; break;
      case 'point.name': if (point && point.evseId === null) point.evseId = text; break;
      case 'lat': if (site) site.lat = numOrNull(text); break;
      case 'lon': if (site) site.lon = numOrNull(text); break;
      case 'postcode': if (site && site.postcode === null) site.postcode = text; break;
      case 'operator.name': if (site && site.operator === null) site.operator = text; break;
      case 'connector.type': if (connector) connector.type = text; break;
      case 'connector.format': if (connector) connector.format = text; break;
      case 'connector.power': if (connector) connector.power = text; break;
      case 'connector.voltage': if (connector) connector.voltage = text; break;
      case 'connector.amperage': if (connector) connector.amperage = text; break;
      default: break;
    }
  }

  parser.on('opentag', (node) => {
    const name = localName(node.name);
    switch (name) {
      case 'energyInfrastructureSite': {
        const rawId = node.attributes.id || node.attributes.ID || null;
        site = {
          id: rawId ? `${ID_PREFIX}${rawId}` : null,
          name: null, lastUpdated: null,
          lat: null, lon: null, address: null, city: null, postcode: null,
          operator: null, connectors: [],
        };
        break;
      }
      case 'operator': inOperator = true; break;
      case 'address': inAddress = true; break;
      case 'addressLine': inAddressLine = true; break;
      case 'name': nameDepth++; break;
      case 'coordinatesForDisplay': inCoordinates = true; break;
      case 'refillPoint':
        point = {
          pointId: node.attributes.id || node.attributes.ID || null,
          evseId: null,
          connectors: [],
        };
        break;
      case 'connector': if (point) connector = {}; break;
      case 'lastUpdated': if (site && !point) startCapture('site.lastUpdated'); break;
      case 'latitude': if (inCoordinates) startCapture('lat'); break;
      case 'longitude': if (inCoordinates) startCapture('lon'); break;
      case 'postcode': if (inAddress) startCapture('postcode'); break;
      case 'connectorType': if (connector) startCapture('connector.type'); break;
      case 'connectorFormat': if (connector) startCapture('connector.format'); break;
      case 'maxPowerAtSocket': if (connector) startCapture('connector.power'); break;
      case 'voltage': if (connector) startCapture('connector.voltage'); break;
      case 'maximumCurrent': if (connector) startCapture('connector.amperage'); break;
      case 'value':
        // Route a `value` to the innermost open context.
        if (inAddressLine && site) startCapture('addressLine');
        else if (inOperator && nameDepth > 0 && site && site.operator === null) startCapture('operator.name');
        else if (point && nameDepth > 0 && point.evseId === null) startCapture('point.name');
        else if (nameDepth > 0 && !inOperator && !inAddress && !point && site && site.name === null) startCapture('site.name');
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
        (name === 'value' && (capture.key === 'site.name' || capture.key === 'operator.name' || capture.key === 'point.name' || capture.key === 'addressLine')) ||
        (name === 'lastUpdated' && capture.key === 'site.lastUpdated') ||
        (name === 'latitude' && capture.key === 'lat') ||
        (name === 'longitude' && capture.key === 'lon') ||
        (name === 'postcode' && capture.key === 'postcode') ||
        (name === 'connectorType' && capture.key === 'connector.type') ||
        (name === 'connectorFormat' && capture.key === 'connector.format') ||
        (name === 'maxPowerAtSocket' && capture.key === 'connector.power') ||
        (name === 'voltage' && capture.key === 'connector.voltage') ||
        (name === 'maximumCurrent' && capture.key === 'connector.amperage');
      if (owns) { applyCapture(capture.key, capture.text); capture = null; }
    }

    switch (name) {
      case 'operator': inOperator = false; break;
      case 'address': inAddress = false; break;
      case 'addressLine': inAddressLine = false; break;
      case 'name': if (nameDepth > 0) nameDepth--; break;
      case 'coordinatesForDisplay': inCoordinates = false; break;
      case 'connector':
        if (point && connector) {
          point.connectors.push({
            standard: mapConnectorType(connector.type),
            format: connector.format || 'unknown',
            powerKW: wattsToKW(connector.power),
            voltage: numOrNull(connector.voltage),
            amperage: numOrNull(connector.amperage),
          });
        }
        connector = null;
        break;
      case 'refillPoint':
        if (site && point) {
          // One refillPoint may expose several connectors. Emit one schema
          // connector per <connector>; suffix the point id when there are >1 so
          // each connector still has a unique join key.
          const multi = point.connectors.length > 1;
          point.connectors.forEach((c, idx) => {
            const pid = point.pointId
              ? (multi ? `${point.pointId}-${idx + 1}` : point.pointId)
              : null;
            site.connectors.push({
              standard: c.standard,
              format: c.format,
              powerKW: c.powerKW,
              voltage: c.voltage,
              amperage: c.amperage,
              pointId: pid,
              evseId: point.evseId || pid,
              status: CHARGER_STATUS.UNKNOWN, // no ES live status
              lastUpdated: site.lastUpdated || null,
            });
            points++;
          });
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
  parser.on('end', () => resolve({ sites, points }));
}

module.exports = {
  name: SOURCE,
  country: COUNTRY,
  hasStatusFeed: false, // Spain: no live-status feed (STATIC ONLY)
  fetch,
  normalize,
  normalizeStreaming,
  parseStatus,
  // exported for unit tests
  _mapConnectorType: mapConnectorType,
  _wattsToKW: wattsToKW,
};
