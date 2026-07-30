/**
 * providers/_shared.js
 *
 * Small helpers shared by DATEX II providers (mobie = PT, dgt = ES). Both feeds
 * use the same connectorType vocabulary and Watt-based power, so the mapping and
 * W->kW conversion live here once. Namespace handling (localName) is here too
 * because every DATEX II feed carries different prefixes for the same elements.
 */

const { CONNECTOR_STANDARD } = require('../schema');

/** Strip an XML namespace prefix ("ns6:foo" / "egi:foo" -> "foo"). */
function localName(qname) {
  const i = qname.indexOf(':');
  return i === -1 ? qname : qname.slice(i + 1);
}

/**
 * Map a DATEX II connectorType token to a canonical connector standard.
 * Shared by MOBI.E and DGT (identical vocabulary, e.g. "iec62196T2",
 * "iec62196T2COMBO", "chademo", "domesticF", "iec60309x2single16").
 * @param {string} raw
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

/** Normalize maxPowerAtSocket (Watts) -> kW rounded to 0.1, or null. */
function wattsToKW(w) {
  const n = parseFloat(w);
  if (!isFinite(n) || n <= 0) return null;
  return Math.round((n / 1000) * 10) / 10;
}

/** parseFloat that yields null instead of NaN. */
function numOrNull(v) {
  const n = parseFloat(v);
  return isFinite(n) ? n : null;
}

/** Pipe a Readable stream or an XML string into a sax stream. */
function pipeInput(input, parser, reject) {
  if (typeof input === 'string') {
    parser.write(input);
    parser.end();
  } else if (input && typeof input.pipe === 'function') {
    input.on('error', reject);
    input.pipe(parser);
  } else {
    reject(new Error('unsupported input (expected Readable stream or string)'));
  }
}

module.exports = { localName, mapConnectorType, wattsToKW, numOrNull, pipeInput };
