/**
 * schema.js
 *
 * The common charger schema every ingest provider normalizes to.
 *
 * This is the INTERNAL representation produced by providers (mobie, later dgt/ocm).
 * The serving function (netlify/functions/mobie-charger-location.js) maps this into
 * the OCPI-ish shape the frontend already consumes. Keeping the two separate means a
 * new country only has to emit this schema — the serving contract never changes.
 *
 * @typedef {Object} Connector
 * @property {string} standard  Canonical connector standard. One of:
 *   "Type 2" (AC), "CCS" (DC), "CHAdeMO" (DC), "Type 1", "Tesla", "Type 3", "Unknown".
 * @property {string} format    Physical format. MOBI.E: "socket" | "cableMode3" | "unknown"; REVE (OCPI): "SOCKET" | "CABLE". Consumers normalize case-insensitively (/socket/, /cable/).
 * @property {string|null} [chargingMode]  IEC 61851 charging mode when known (MOBI.E): "mode2AC1p" | "mode3AC3p" | "mode4DC" — carries AC phase count and AC/DC.
 * @property {{energy?:number,time?:number,flat?:number}} [opc]  Per-station operator fee (MOBI.E "Descarregar Tarifas" CSV, REGULAR): ENERGY €/kWh, TIME €/min, FLAT €/charge. Input to CEME community-pricing cards. Present only for matched PT stations.
 * @property {number|null} powerKW  Max power at socket, kilowatts (W / 1000).
 * @property {number|null} voltage  Nominal voltage (V), or null.
 * @property {number|null} amperage Maximum current (A), or null.
 * @property {string} pointId   Provider refill-point id used to join live status (e.g. "AMD-00051-1").
 * @property {string} evseId    Human-readable external identifier (e.g. "PT*EZC*E*AMD*00051*1").
 * @property {string} status    Live status for this connector (see ChargerStatus). Defaults to "unknown".
 * @property {string|null} lastUpdated ISO timestamp of last status/spec update, or null.
 * @property {string} [lastBusyAt]  ISO timestamp of the last poll where this connector was observed entering "charging" (session start) — the real "last used" moment, accurate to the status-ingest cadence. Absent until first observed in use.
 * @property {Object[]} [tariffs]  Dynamic pricing (OCPI-shaped: {type:"AD_HOC_PAYMENT", currency, elements[].price_components[]} with numeric ENERGY/TIME prices). Present only when the provider joins per-connector tariff data (e.g. REVE /connectors/tariffs).
 *
 * @typedef {Object} Charger
 * @property {string} id        Stable unique id (provider site id, e.g. "EZC-AMD-00051").
 * @property {string} country   ISO 3166-1 alpha-2, uppercase (e.g. "PT").
 * @property {string} source    Provider name (e.g. "mobie").
 * @property {number} lat       Latitude (WGS84).
 * @property {number} lon       Longitude (WGS84).
 * @property {string} name      Display name.
 * @property {string} address   Street address (single line), may be "".
 * @property {string} [city]    City / locality.
 * @property {string} [postcode] Postal code.
 * @property {string} operator  Charge-point operator display name.
 * @property {boolean|null} [open24h]  True if chargeable around the clock — a 24/7 venue (DATEX II OpenAllHours / OCPI twentyfourseven) OR a charger usable while the venue is closed (OCPI charging_when_closed); false if only during venue hours; null if unknown.
 * @property {boolean} [chargingWhenClosed]  True only when open24h is true because the charger works while the venue itself may be closed (not a true 24/7 venue) — lets the UI flag that on-site amenities might be unavailable.
 * @property {string[]} [facilities]  On-site facility tags (OCPI Location.facilities, e.g. "RESTAURANT","SUPERMARKET","WIFI"). Empty array or absent when unknown.
 * @property {string[]} [capabilities]  Site-level union of OCPI EVSE capabilities (e.g. "CREDIT_CARD_PAYABLE","CONTACTLESS_CARD_SUPPORT","RFID_READER","RESERVABLE"). Stored raw; empty when unknown.
 * @property {string|null} [parkingType]  OCPI parking_type (e.g. "ON_STREET","PARKING_GARAGE"), or null.
 * @property {Connector[]} connectors  All connectors / refill points at this site.
 * @property {Object|null} tariff  Raw tariff hint (currency, pricingPolicy, minimumDeliveryFee) or null.
 * @property {string} status    Aggregate site status (see ChargerStatus).
 * @property {string} lastUpdated ISO timestamp of the freshest update at this site.
 *
 * ChargerStatus enum (design doc):
 *   available | charging | outOfOrder | planned | removed | unknown
 * (MOBI.E also emits: inoperative | blocked — mapped onto the above in the provider.)
 */

/** Canonical status values used across the pipeline. */
const CHARGER_STATUS = Object.freeze({
  AVAILABLE: 'available',
  CHARGING: 'charging',
  OUT_OF_ORDER: 'outOfOrder',
  PLANNED: 'planned',
  REMOVED: 'removed',
  UNKNOWN: 'unknown',
});

/** Canonical connector standards used across the pipeline. */
const CONNECTOR_STANDARD = Object.freeze({
  TYPE2: 'Type 2',
  TYPE1: 'Type 1',
  TYPE3: 'Type 3',
  CCS: 'CCS',
  CHADEMO: 'CHAdeMO',
  TESLA: 'Tesla',
  UNKNOWN: 'Unknown',
});

module.exports = { CHARGER_STATUS, CONNECTOR_STANDARD };
