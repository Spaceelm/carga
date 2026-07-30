# carga-ingest

Scheduled ingest of national EV-charger feeds into Upstash Redis, for the [carga](https://github.com/Spaceelm/carregar-WEB) web app.

This runs on GitHub Actions. It lives in its own **public** repo because Actions minutes are free and unmetered for public repositories — the app repo is private and was hitting the 2,000 min/month Free-plan cap (minutes are pooled per account, so moving to another *private* repo would not have helped).

## What it does

| Country | Source | Static | Live status |
|---|---|---|---|
| PT | MOBI.E NAP (DATEX II v3) | `evChargingInfra` (~183 MB) | `evActualStatus` (~40 MB), joined by refill-point id |
| ES | REVE (OCPI 2.2) + DGT | `/locations` (rate-limited, resumable crawl) | `/markers` public endpoint, tiled sweep |

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
| Full refresh (PT + ES matrix) | `0 4 * * *` | daily, after MOBI.E's ~03:00 publish |
| PT status | `*/15 * * * *` | every 15 min |
| ES status sweep | `5 * * * *` | hourly |
| ES crawl chunk | `25 */3 * * *` | every 3 h |
| Heartbeat | `0 3 1 * *` | monthly |

Why not faster:

1. **Upstash 500k commands/month**, shared with the app's read path. Current ingest load ≈ 120k/month. The expensive `MGET`-all + history roll is gated to once per hour inside `run.js`; the other PT runs take a cheap delta path (~6 commands), which is what makes 15-min affordable.
2. **Upstream politeness.** Every PT status run pulls MOBI.E's ~40 MB feed (~115 GB/month at this cadence). REVE's official API is documented at ~5 req/hr. Getting throttled or blocked upstream would break live availability in the app.
3. **GitHub crons** have 5-min granularity and are queued 5–20 min late under load, so sub-15-min is unreliable anyway.

The **heartbeat** matters: GitHub disables scheduled workflows in public repos after **60 days of no repository activity**. The monthly job commits a timestamp to reset that clock. If the crons ever go silent, check whether they were auto-disabled (the Actions tab shows a banner and re-enabling is one click).

## Required secrets

Set under *Settings → Secrets and variables → Actions*:

| Secret | Used by | Notes |
|---|---|---|
| `UPSTASH_REDIS_REST_URL` | all jobs | must be the **same** DB the web app reads |
| `UPSTASH_REDIS_REST_TOKEN` | all jobs | write access |
| `REVE_API_KEY` | ES full/crawl only | official REVE OCPI API; the public `/markers` status sweep needs no key |

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

## Shared file: `netlify/functions/_reve.js`

This is a **mirrored copy** of the same file in the app repo. It's shared because the app's serving function needs it at request time (for the ES live-status overlay) *and* the ingest needs it (`providers/reve.js` requires it by relative path).

It is kept at the identical relative path so the two copies are byte-identical and syncing is a plain `cp`. **If you change it in one repo, mirror it to the other.** It is the only duplicated file.
