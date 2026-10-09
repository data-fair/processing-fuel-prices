# @data-fair/processing-fuel-prices

Data-fair processing plugin that publishes the fuel prices of French gas stations, from the
["Prix des carburants" instant feed](https://donnees.roulez-eco.fr/opendata/instantane) of the
French Ministry of Economy, into a data-fair REST dataset (one line per station and fuel type).

## How it works

- **Create mode**: creates the REST dataset, sends every line of the feed, then switches the
  processing config to update mode.
- **Update mode**: only processes the lines updated in the feed since the last synchronization.
  New and changed lines are upserted, unchanged ones are skipped, and lines that left the feed
  are deleted.

The last synchronization date is stored in the processing config (`lastSync`, "Date de la
dernière synchronisation" in the form) and only advances once every line of a run has been
sent. If a run fails or is stopped midway, the next run sends the same lines again. To catch up
after an incident, set that field to an earlier date and run the processing.

Batches that fail on transient errors (HTTP 429/5xx, for instance an overloaded
Elasticsearch) are sent again up to 3 times. Deleting a line that is already gone is not an
error.

## Development

- Node 24 (`nvm use`), native TypeScript (no build step).
- `npm install`
- `npm run build-types` generates the types from the JSON schemas.
- `npm test` runs the `node:test` suite (`test-it/`, no data-fair instance needed).
- `npm run lint` / `npm run lint-fix`.

## Release

Publishing to the registry is done by GitHub Actions: a push on `master` publishes to staging,
a `v*` tag publishes to production. Never publish manually.
