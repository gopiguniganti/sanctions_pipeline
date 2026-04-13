# Cluster configuration notes

`cluster-config.json` is a **job cluster** config, not an all-purpose cluster —
that distinction matters and is worth being able to explain:

- **Job cluster vs. all-purpose cluster**: a job cluster spins up for the
  duration of a scheduled run and terminates immediately after, so you pay
  for compute only while the pipeline is actually running. An all-purpose
  cluster stays warm for interactive notebook work (exploration, ad hoc
  queries) and costs more per DBU as a result. This pipeline is a scheduled
  batch job (bronze/silver/gold run daily against a refreshed SDN export), so
  a job cluster is the right fit — an all-purpose cluster would only make
  sense during active development, when you're iterating cell-by-cell.

- **Autoscaling (1–4 workers)**: this workload is small (a single sanctions
  list, a modest customer file) but the design should hold if it scaled up to
  a full customer book. Autoscaling with a low floor keeps cost down on light
  days and lets Spark add workers automatically if a run needs to process a
  larger backlog (e.g. after a multi-day gap or a bulk customer onboarding).

- **Spot with on-demand fallback**: `SPOT_WITH_FALLBACK` with
  `first_on_demand: 1` keeps the driver on stable on-demand capacity (losing
  the driver kills the whole job) while letting worker nodes use cheaper spot
  capacity, since losing a worker just triggers Spark's normal retry/
  re-scheduling. This is a meaningful cost lever on non-latency-critical
  batch workloads — production real-time monitoring pipelines would make a
  different call here in favor of stability.

- **`autoCompact` / `optimizeWrite`**: enabled at the cluster level so small
  files get compacted automatically on write, complementing the explicit
  `OPTIMIZE ... ZORDER BY` run in the silver notebook. The explicit ZORDER
  step is still needed because auto-compaction doesn't re-cluster data by a
  specific column — it just merges small files.

- **`autotermination_minutes: 20`**: idle-shutdown safety net in case a job
  run hangs or a notebook is left attached manually during development.

None of this is tuned against real production transaction volume — it's
sized for this sample project. At BMO's actual data volumes, the same
levers (autoscale ceiling, node type, spot ratio) would need to be
re-benchmarked against real throughput, not assumed.
