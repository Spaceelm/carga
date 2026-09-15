# carga-ingest

Scheduled ingest of national EV-charger feeds into Upstash Redis, for the [carga](https://github.com/Spaceelm/carregar-WEB) web app.

This runs on GitHub Actions. It lives in its own **public** repo because Actions minutes are free and unmetered for public repositories — the app repo is private and was hitting the 2,000 min/month Free-plan cap (minutes are pooled per account, so moving to another *private* repo would not have helped).

## What it does

| Country | Source | Static | Live status |
|---|---|---|---|
| PT | MOBI.E NAP (DATEX II v3) | `evChargingInfra` (~183 MB) | `evActualStatus` (~40 MB), joined by refill-point id |
| ES | REVE (OCPI 2.2) + DGT | `/locations` (rate-limited, resumable crawl) | `/markers` public endpoint, tiled sweep |
| FR | transport.data.gouv.fr (national IRVE) | BETA consolidation res. `84013` (~120 MB CSV) | consolidated `IRVE dynamique` res. `84098` (~9 MB CSV), joined by `id_pdc_itinerance` |

It writes a per-country Redis keyspace (`chargers:<cc>:geo`, `charger:<cc>:<id>`, plus index/meta keys). Nothing else.

## Contract with the web app — do not break these

The app reads Redis directly via its Netlify function and never calls this repo. The only coupling is the **data contract**:

- **Keyspace layout** — `scripts/ingest/store.js` (`chargers:<cc>:geo`, `charger:<cc>:<id>`, `pointindex`, `coordindex`, `lastfeed`, `meta`).
- **Record schema** — `scripts/ingest/schema.js`. The app's `toStation()` maps these fields; adding fields is safe, renaming/removing is not.
- **Same Upstash database** as the app's `UPSTASH_REDIS_REST_URL`.

Change either side of that contract and you must change both repos.

## Schedules

Set by the limits that actually bind (Actions minutes are free here):

| Job | Cron (UTC) | Cadence |
|---|---|---|
| Full refresh (PT + ES + FR matrix) | `13 4 * * *` | daily, after MOBI.E's ~03:00 publish |
| PT status | `2,12,22,32,42,52 * * * *` | every 10 min |
| ES status sweep | `5 * * * *` | hourly |
| FR status | `8,18,28,38,48,58 * * * *` | every 10 min |
| ES crawl chunk | `25 */3 * * *` | every 3 h |
| Heartbeat | `47 3 1 * *` | monthly |

Why PT and FR can run at 10 min:

**The expensive work is gated on wall-clock, not on cadence.** The `MGET`-all + history
roll happens once per history window inside `run.js` — hourly for PT, every 3 h for FR
(`provider.historyEveryHours`). That is 24 and 8 sweeps a day no matter how often the
cron fires. Every other run takes the delta path.

**And a faster poll makes each delta smaller.** Measured against the live FR feed, ~1.5%
of rows change in 10 minutes vs ~4.0% in 30. Three times the runs, each patching about a
third as many records, so the `MGET`/`MSET` work per day barely moves. The fixed
per-run overhead is what had to come down — see below.

Why not faster still:

1. **Upstash 500k commands/month**, shared with the app's read path. Ingest sits around
   120k/month; the optimizations below are what keep it there at 10 min.
2. **Upstream politeness.** Every PT status run pulls MOBI.E's ~40 MB feed — ~180 GB/month
   at this cadence. The feed republishes about every 5 minutes (its DATEX `publicationTime`
   is typically 3–5 min old), so 10 min is already near the point of diminishing returns;
   polling faster would mostly re-download bytes we have.
3. **GitHub crons** have 5-min granularity and are queued 5–20 min late under load, so a
   sub-10-min nominal cadence would not actually deliver sub-10-min data.
4. **ES is deliberately left hourly** — see the note below. It is not a budget question.

### Keeping a status run cheap

A status run's cost is `fixed overhead + work proportional to what changed`. At 10 min the
overhead term is what dominates, so it was cut from ~12 Redis commands to ~4:

- **One `MGET` for the prologue.** `meta`, `pointindex` and `lastfeed` were three separate
  `GET`s; they are now one round trip (`chunkedGetFrom` resolves a chunk sentinel from
  bytes already in hand).
- **No `SMEMBERS` on the delta path.** The id set is only consumed by the full path, so it
  is read there instead of up front.
- **A compact last-feed snapshot.** This was the big one. The snapshot used to be a
  `{pointId: status}` map — ~1.6 MB for FR, which `chunkedSet` splits into several chunks,
  and chunk *writes* cannot be batched into one `MSET` (each chunk is already sized against
  Upstash's ~1 MB request cap). It cost ~5 commands to write and 2 to read on every run.
  The ids in it were pure redundancy: the point index already enumerates the same points in
  the same order. Statuses are now stored positionally, one character per point — FR's
  ~165k points become a ~165k-char string: one plain `SET`, one plain `GET`, never chunked.
  A fingerprint of the point-index key list is stored alongside; when a full refresh
  rebuilds the index the fingerprint stops matching and the snapshot is rejected, so the run
  takes the always-correct full path and re-seeds it. Losing a snapshot is only ever a cost
  problem, never a correctness one.
- **Quiet runs write nothing.** If the encoded snapshot is identical to the stored one, the
  write is skipped entirely.

Measured against a simulated keyspace (PT 8k stations / full feed coverage; FR 47k stations
/ ~35% of points carrying a dynamic row), counting Redis **commands**, which is what Upstash
bills:

| | delta run | full (history) run | runs/day | cmds/day |
|---|---|---|---|---|
| PT before, 15 min | 12 | 86 | 96 | 2928 |
| PT after, 10 min | **6** | 85 | 144 | **2760** (−6%) |
| FR before, 30 min | 13 | 333 | 48 | 3184 |
| FR after, 10 min | **6** | 331 | 144 | **3464** (+9%) |

Net for the two countries: **+1.8% commands for 2–3x the polling rate**. A run where nothing
changed at all costs 3 commands, down from 16. The absolute daily figures depend on the real
station counts and how often statuses genuinely flip, so treat them as a ratio rather than a
forecast — and watch the Upstash dashboard after the cadence change lands.

The **heartbeat** matters: GitHub disables scheduled workflows in public repos after **60 days of no repository activity**. The monthly job commits a timestamp to reset that clock. If the crons ever go silent, check whether they were auto-disabled (the Actions tab shows a banner and re-enabling is one click).

## Required secrets

Set under *Settings → Secrets and variables → Actions*:

| Secret | Used by | Notes |
|---|---|---|
| `UPSTASH_REDIS_REST_URL` | all jobs | must be the **same** DB the web app reads |
| `UPSTASH_REDIS_REST_TOKEN` | all jobs | write access |
| `REVE_API_KEY` | ES full/crawl only | official REVE OCPI API; the public `/markers` status sweep needs no key |

France needs **no key at all** — the national IRVE files are Licence Ouverte 2.0 open data.

Forks never receive secrets, and scheduled runs only execute on the default branch — a public repo is safe here. Never commit a `.env`.

## Running locally

```bash
npm ci
UPSTASH_REDIS_REST_URL=... UPSTASH_REDIS_REST_TOKEN=... npm run ingest -- --country=PT --status-only --dry-run
```

`--dry-run` performs no writes. Flags: `--country=<cc>`, `--status-only`, `--dry-run`.

```bash
npm test
```

## France specifics

France publishes by decree, so the data is genuinely open (no key, no registration)
but **highly fragmented at the source**: ~279 separate producers publish their own
files to data.gouv.fr, which the National Access Point deduplicates into the single
consolidation we read. Measured on the live feeds (2026-07-31):

- **164,629 charge points / 47,350 stations** — ~6x Portugal.
- **Live status covers ~33% of connectors.** The dynamic file lists ~124k points,
  but only ~57k rows are fresher than 24h; the rest are unmaintained producers,
  some last updated in 2020. Stale rows are dropped, so those points read
  `unknown` (grey) rather than falsely "available" — the same only-confirmed-counts
  policy used for Spain.
- **Richer static schema than PT/ES**: payment methods (`paiement_cb`), free-charging
  flag, opening hours, reservation, PMR accessibility, vehicle-size restrictions and
  free-text tariffs. We currently map hours, payment/reservation capabilities,
  parking type and the free-charging flag; `tarification` is free text (only ~22%
  populated, no consistent grammar) and is deliberately **not** parsed into prices.
- **Two known feed defects**, both handled in `providers/fr.js`: ~900 rows publish
  **watts** in the kW column (values > 1000 are divided by 1000), and
  `cable_t2_attache` is 100% empty in the consolidation (so AC connectors report an
  `unknown` format rather than guessing socket-vs-tethered).

### Why FR needs chunked indexes

`store.js` writes a point index (charge point → station) at every full refresh. PT's
fits in one value; FR's ~165k entries serialize past **Upstash's ~1 MB request cap**,
which would fail the write outright. `chunkedSet`/`chunkedGet` split oversized values
across `<key>:c<n>` keys behind a `{"__chunks":N}` sentinel. Values under the
threshold are still written as a single plain SET, so PT and ES are byte-for-byte
unchanged and no migration was needed. A partially-written or partially-missing chunk
set reads back as `null`, which callers already treat as "index absent" and fall back
to the always-correct full path.

### FR budget note

FR rolls availability history every **3 hours** (`provider.historyEveryHours = 3`)
instead of hourly. The history roll is the expensive part — it MGETs every station —
and 47k stations hourly would dominate the 500k/month Upstash budget on its own.
`accumulateHistory` spreads each observation across the three hourly buckets the
window covers, so the 7x24 profile has no gaps, just 3-hour resolution. Between rolls
the cheap delta path touches only the charge points whose status actually changed.

This gate is why FR can poll at the same 10 min as PT while rolling history 3x less
often: the cadence controls how fresh `status` is, `historyEveryHours` controls how
often the costly sweep runs, and the two are independent.

One caveat worth knowing when reading the data: only about half of the ~121k rows in
the dynamique file carry a timestamp inside `STALE_HOURS` (24 h). The rest are dropped
by the provider and read as "unknown" rather than as a stale availability claim, so a
faster cadence does not widen coverage — it only sharpens the subset that is genuinely
live.

### Why ES is not on 10 minutes

ES is the one country where the cadence is not a budget decision, and `runAreaStatus`
in `providers/reve.js` should not simply be re-pointed at a faster cron:

1. **It has no history gate.** It calls `accumulateHistory` unconditionally on every
   matched record. History buckets are per UTC hour, so six runs an hour would EWMA six
   observations into the same bucket; at `HISTORY_ALPHA = 0.25` the prior value keeps
   only `0.75⁶ ≈ 18%` of its weight and the week-long profile collapses into "whatever
   the last hour looked like". This fails silently — the map just starts showing wrong
   typical-availability.
2. **It has no delta path.** Every run is a full MGET + MSET over all matched stations.
3. **Request volume.** REVE has no bulk status feed, so each sweep is one POST per
   populated ~28 km tile. Six sweeps an hour against an undocumented public endpoint is
   the most likely way to get blocked — and `fetchReveStatus` swallows failures, so
   being throttled would surface as chargers quietly going "unknown", not as a red run.

Fixing (1) is worth doing on its own merits regardless of cadence.

## Shared file: `netlify/functions/_reve.js`

This is a **mirrored copy** of the same file in the app repo. It's shared because the app's serving function needs it at request time (for the ES live-status overlay) *and* the ingest needs it (`providers/reve.js` requires it by relative path).

It is kept at the identical relative path so the two copies are byte-identical and syncing is a plain `cp`. **If you change it in one repo, mirror it to the other.** It is the only duplicated file.
