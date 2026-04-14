# Sanctions Screening Pipeline — Databricks / Delta Lake

A small bronze/silver/gold pipeline that ingests the OFAC Specially
Designated Nationals (SDN) sanctions list, cleans and upserts it into Delta,
and screens a sample customer list against it with a self-built fuzzy-match
scorer.

Built as a scoped, finishable companion to production AML work — real public
data, real Delta Lake mechanics (`MERGE INTO`, `OPTIMIZE`/`ZORDER BY`,
`DESCRIBE HISTORY`), not a toy tutorial.

## Architecture

```
OFAC SDN.CSV (public, daily-updated)
        │
        ▼
  ┌─────────────┐   append-only, string-typed, lineage-stamped
  │   BRONZE    │   notebooks/01_bronze_ingest.py
  └─────────────┘
        │
        ▼
  ┌─────────────┐   cleaned, standardized, MERGE INTO upsert
  │   SILVER    │   OPTIMIZE + ZORDER, DESCRIBE HISTORY
  └─────────────┘   notebooks/02_silver_transform.py
        │
        ▼
  ┌─────────────┐   fuzzy-match customers vs. sanctions entities
  │    GOLD     │   scored alerts above threshold
  └─────────────┘   notebooks/03_gold_screening.py
```

## Repo layout

```
src/sanctions_pipeline/   Shared pipeline logic (used by both notebooks and local tests)
  bronze.py               Raw ingestion
  silver.py                Cleaning + Delta MERGE INTO / OPTIMIZE / DESCRIBE HISTORY
  screening.py             Fuzzy-match scoring logic
notebooks/                Databricks notebooks (paste directly into Databricks Community Edition)
data/                     Sample SDN + sample customer CSVs (see note below)
cluster/                  Cluster config + tuning rationale
local_test/               Local functional test (see "Local testing" below)
```

## Running this in Databricks Community Edition

1. Create a free Community Edition workspace at
   https://community.cloud.databricks.com
2. Add this repo via **Repos → Add Repo** (or upload the `notebooks/` and
   `src/` folders manually).
3. Attach a cluster (or let each notebook's job-cluster config in
   `cluster/cluster-config.json` guide a manual cluster setup — Community
   Edition doesn't support job clusters directly, so use an all-purpose
   cluster with similar sizing for testing).
4. Update the `sys.path.append(...)` line at the top of each notebook to
   point at wherever you cloned `src/` in your Workspace.
5. Run `01_bronze_ingest.py` → `02_silver_transform.py` → `03_gold_screening.py`
   in order. Each notebook exposes its paths as widgets at the top if you
   want to change them.

Databricks' driver nodes have unrestricted internet access, so
`01_bronze_ingest.py` downloads the live SDN list directly from
`sanctionslistservice.ofac.treas.gov` — no manual download step needed.

## Local testing

`local_test/run_local.py` runs the bronze-read → silver-clean → gold-screen
logic locally against the sample data in `data/`, using plain PySpark +
Parquet rather than Delta.

**Why not Delta locally:** `delta-spark`'s Python package is a thin wrapper —
the actual JVM jar is fetched at runtime from Maven Central. If your machine
can reach Maven Central, you can run the real Delta path locally too (see the
commented `delta-spark` line in `requirements.txt`); this repo's own dev
environment was network-restricted to PyPI/npm/GitHub, so the local test
validates the transformation and fuzzy-matching logic using Parquet as a
stand-in, and the Delta-specific commands (`MERGE INTO`, `OPTIMIZE ZORDER BY`,
`DESCRIBE HISTORY`) are written in the notebooks exactly as they run in
Databricks, but only actually exercised there.

```bash
pip install -r requirements.txt
python local_test/run_local.py
```

Sample output: cleans 10 sample sanctions entities, screens 10 sample
customers, and correctly flags an exact-name match and a rearranged-name
match while correctly skipping weaker candidates below the match threshold.

## Sample data note

`data/sample_sdn.csv` and `data/sample_customers.csv` are small, hand-built
files matching the real OFAC SDN CSV schema (`ent_num`, `SDN_Name`,
`SDN_Type`, `Program`, `Remarks` with `a.k.a.` aliases embedded) — they are
**not** a scrape or reproduction of the live OFAC list, just structurally
representative sample rows for local development and testing without needing
network access to Treasury's servers. The Databricks notebooks pull the real,
live, public SDN list directly.

## Known limitations (worth naming, not hiding)

- **Screening is an O(n × m) cross-join** — fine at sample scale, but a real
  customer book against the full SDN list would need blocking (first-letter
  or phonetic-key pre-filtering) before scoring pairs. Noted in
  `notebooks/03_gold_screening.py`.
- **Fuzzy-match logic is self-built**, not a reproduction of any vendor
  screening engine's internal matching algorithm (e.g. Bridger, NetReveal).
  It's built to understand the mechanics of the same category of problem.
- **Cluster sizing in `cluster/cluster-config.json` is not benchmarked**
  against real production transaction volume — see `cluster/README.md`.

## Tech

Databricks · Delta Lake (`MERGE INTO`, `OPTIMIZE`/`ZORDER BY`,
`DESCRIBE HISTORY`, time travel) · PySpark · rapidfuzz
