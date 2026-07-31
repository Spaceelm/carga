/**
 * providers/index.js
 *
 * Provider registry: country (ISO 3166-1 alpha-2) -> provider module.
 * Each provider exports { name, country, fetch, normalize, normalizeStreaming }.
 *
 * Registered: PT -> mobie, ES -> dgt.
 * Future (design doc, out of scope now — do NOT implement here):
 *   '*' -> require('./ocm')     // OpenChargeMap fallback / gap-filler
 * When adding one, register it below and enable its country in the workflow.
 */

const mobie = require('./mobie');
const reve = require('./reve');
const fr = require('./fr');

/** @type {Record<string, {name:string, country:string, fetch:Function, normalize:Function, normalizeStreaming:Function}>} */
const PROVIDERS = {
  PT: mobie,
  // ES static registry: REVE official OCPI API (replaces the DGT DATEX II feed).
  // providers/dgt.js is kept in the tree for reference but is no longer registered.
  ES: reve,
  // FR: national IRVE open data (transport.data.gouv.fr) — deduplicated static
  // consolidation + Gireve/QualiCharge-fed dynamic status. No API key.
  FR: fr,
};

// Fallback provider used when no country-specific provider exists.
// Left null intentionally until OCM is implemented.
const FALLBACK_PROVIDER = null; // require('./ocm')

/**
 * Resolve a provider for a country code.
 * @param {string} country ISO 3166-1 alpha-2 (case-insensitive).
 * @returns {{name:string, country:string, fetch:Function, normalize:Function, normalizeStreaming:Function}}
 * @throws if no provider is registered and no fallback exists.
 */
function getProvider(country) {
  if (!country) throw new Error('getProvider: country is required');
  const cc = String(country).toUpperCase();
  const provider = PROVIDERS[cc] || FALLBACK_PROVIDER;
  if (!provider) {
    const known = Object.keys(PROVIDERS).join(', ') || '(none)';
    throw new Error(`No charger provider registered for country "${cc}". Known: ${known}.`);
  }
  return provider;
}

/** List registered country codes. */
function supportedCountries() {
  return Object.keys(PROVIDERS);
}

module.exports = { getProvider, supportedCountries, PROVIDERS };
