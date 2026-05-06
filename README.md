# Sanctions Screening Pipeline

A bronze/silver/gold pipeline on Databricks and Delta Lake that ingests the
OFAC Specially Designated Nationals (SDN) sanctions list, cleans and upserts
it into Delta, and screens a customer list against it using a fuzzy-match
scorer.

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
src/sanctions_pipeline/   Shared pipeline logic (used by notebooks and local tests)
  bronze.py               Raw ingestion
  silver.py               Cleaning + Delta MERGE INTO / OPTIMIZE / DESCRIBE HISTORY
  screening.py            Fuzzy-match scoring logic
notebooks/                Databricks notebooks
data/                     Sample SDN + customer CSVs for local testing
cluster/                  Cluster config + sizing notes
local_test/               Local functional test
```

## Running in Databricks

1. Create a workspace at https://community.cloud.databricks.com (or use an
   existing one).
2. Add this repo via **Repos → Add Repo** (or upload the `notebooks/` and
   `src/` folders manually).
3. Attach a cluster — `cluster/cluster-config.json` has the sizing used for
   this pipeline; adapt it to an all-purpose cluster if you're on Community
   Edition.
4. Update the `sys.path.append(...)` line at the top of each notebook to
   point at wherever `src/` lives in your workspace.
5. Run `01_bronze_ingest.py` → `02_silver_transform.py` → `03_gold_screening.py`
   in order. Each notebook exposes its paths as widgets at the top.

`01_bronze_ingest.py` downloads the live SDN list directly from
`sanctionslistservice.ofac.treas.gov` — no manual download step needed.

## Local testing

`local_test/run_local.py` runs the same bronze → silver → gold logic locally
against the sample data in `data/`, using plain PySpark and Parquet instead
of Delta (Delta's JVM jar is fetched from Maven Central at runtime, which
this dev environment didn't have access to). The Delta-specific commands
(`MERGE INTO`, `OPTIMIZE ZORDER BY`, `DESCRIBE HISTORY`) live in the
notebooks and run as written when deployed to Databricks.

```bash
pip install -r requirements.txt
python local_test/run_local.py
```

This cleans 10 sample sanctions entities, screens 10 sample customers, and
flags an exact-name match and a rearranged-name match while skipping
candidates below the match threshold.

## Sample data

`data/sample_sdn.csv` and `data/sample_customers.csv` are small, hand-built
files matching the real OFAC SDN schema (`ent_num`, `SDN_Name`, `SDN_Type`,
`Program`, `Remarks` with `a.k.a.` aliases embedded), used for local
development without needing network access. The Databricks notebooks pull
the real, live SDN list directly.

## Known limitations

- **Screening is an O(n × m) cross-join** — fine at sample scale, but a full
  customer book against the full SDN list would need blocking (first-letter
  or phonetic-key pre-filtering) before scoring pairs.
- **The fuzzy-match scorer is a custom implementation**, tuned for this
  dataset's scale rather than production accuracy or throughput.
- **Cluster sizing in `cluster/cluster-config.json` is a starting point**,
  not benchmarked against production transaction volume.

## Tech

Databricks · Delta Lake (`MERGE INTO`, `OPTIMIZE`/`ZORDER BY`,
`DESCRIBE HISTORY`, time travel) · PySpark · rapidfuzz
