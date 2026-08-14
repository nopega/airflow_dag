# DAGs and pipeline code

**This directory mirrors the contents of `github.com/nopega/airflow_dag` (branch
`main`).** Airflow reads its DAGs from that repo by git-sync, not from this one
— `dags.gitSync.repo` in `04_airflow_prod/chart/airflow/values.yaml`, with
`subPath` empty, so the repo's **root** is the DAGs folder.

Nothing here takes effect until it is pushed there.

```
airflow_dag/                     <- repo root == Airflow's DAGs folder
  platform_smoke_test.py
  nyc_taxi_medallion.py          the DAG
  spark/*.yaml                   SparkApplication templates, Jinja-rendered
```

The PySpark jobs are **not here**. They live in `github.com/nopega/data-pipeline`
— mirrored in this project under `pipeline_repo/`, with the reasoning in its
README. Short version: pushing to this repo means telling Airflow what to run,
which is a different level of trust from changing a filter in a Spark job.

## The two paths code takes to a Spark pod

This is the part worth understanding, because the same files reach the cluster
two different ways and a change to one path does not affect the other.

| what | how it gets there | when it takes effect |
|---|---|---|
| `nyc_taxi_medallion.py`, `spark/*.yaml` | git-sync into the Airflow pods | next scheduler parse, ~1 min after push |
| `pipelines/taxi/*.py` (other repo) | Spark fetches them over https from `nopega/data-pipeline` at submit time | next task run |

The DAG and the templates are read by **Airflow**. The pipeline code is read by
the **Spark driver**, which Airflow never touches. That is why a fix to
`silver_clean.py` needs no image rebuild and no Airflow restart — only a push
and a re-run.

The image supplies the runtime (Spark 4.0.1, Iceberg jars, AWS SDK, Python and
its pinned libraries). Git supplies the logic. The image tag answers "what
could this have run with"; the git ref answers "what did it run".

## Running it

The DAG is `@monthly` with `catchup=False`. A scheduled run processes the month
**two months before** its own interval, because TLC publishes with that lag —
ask for a more recent month and CloudFront answers 403, which reads like a
permissions problem and is not.

To load one specific month, trigger with config:

```json
{ "month": "2024-01" }
```

Other params, all overridable per run:

| param | default | what it pins |
|---|---|---|
| `month` | derived from the run | which month to process |
| `image_tag` | `v1.0.2` | the runtime |
| `pipeline_repo` | `nopega/data-pipeline` | where the logic comes from |
| `pipeline_ref` | `refs/heads/main` | which commit of it — set to `<sha>` to pin a run exactly |

## The three tasks

**bronze** → `data_platform.bronze.transactional.taxi_trip`

Downloads the published Parquet and writes it unchanged, plus `_ingest_month`,
`_ingested_at`, `_source_file`, `_run_id`. Partitioned on `_ingest_month`,
which comes from the **argument** rather than from the data — a row with a
corrupt pickup timestamp still lands in the partition it was ingested with,
instead of creating a partition for the year 2098.

The driver streams the file through pyarrow one row group at a time. Reading a
busy month whole would need ~4 GB of driver heap; the batch loop holds ~400k
rows and makes the memory profile independent of the month's size.

**silver** → `data_platform.silver.derived.taxi_trip_cleaned`

Eleven quality rules, **each one counted and printed**. That matters more than
it looks: a rule that quietly starts dropping 40% of a month is
indistinguishable downstream from a quiet month. The task refuses to publish if
fewer than half the rows survive.

Also de-duplicates, resolves `payment_type` from an integer code to a name, and
derives `trip_duration_min` and `avg_speed_mph`.

**gold** → `data_platform.gold.aggregate.taxi_daily_zone_revenue`

Day × pickup zone × payment method. ~10,000 rows a month instead of ~3,000,000.

Shaped for Power BI **Import** mode rather than DirectQuery, and that shapes
every decision in it:

- **pre-aggregated**, so the import is instant and the report does not send a
  query to Trino on every slicer click
- **pre-joined** — the zone lookup is resolved here, so the report has borough
  and zone names as plain columns and needs no relationships modelled
- **`tip_pct` computed as a ratio of sums**, not an average of per-trip
  percentages. Averaging the ratio would let a \$4 trip with a \$2 tip count as
  much as a \$200 trip with a \$10 tip, and the headline number would be wrong
  in a way that looks entirely plausible.

The task reconciles its own trip count against silver and fails if they differ,
because a join that silently drops rows produces a revenue dashboard that is
understated with nothing to show for it.

## Idempotency

Every task deletes the month it is about to write, first. Airflow retries
tasks; an append-only task retried after a partial write leaves duplicates that
no error mentions — they surface as a revenue figure that is 1.4× too high.
Deleting first means running a task twice leaves the same table as running it
once.

## What Airflow is allowed to do

Its ServiceAccount can create `SparkApplication` objects and read pods. It
**cannot create pods**:

```bash
kubectl auth can-i create pods -n spark \
  --as=system:serviceaccount:airflow:airflow-worker      # no
kubectl auth can-i create sparkapplications -n spark \
  --as=system:serviceaccount:airflow:airflow-worker      # yes
```

So a compromised DAG can ask the cluster for a Spark job — a known image
running known code — but cannot start a container of its own choosing.

## Failure modes and where they actually show up

| symptom | cause |
|---|---|
| driver dies at once, 404 on `raw.githubusercontent.com` | `pipeline_repo`/`pipeline_ref` wrong, or the repo is private |
| `ModuleNotFoundError: common` | a shared module was added but not listed in `deps.pyFiles` |
| driver `Pending`, task hangs | no room on `workload=critical`; `kubectl describe pod` names the reason |
| `Initial job has not accepted any resources` | executors Pending — Spot node still starting, or the autoscaler is not running |
| `403` from CloudFront in bronze | asked for a month TLC has not published |
| `NoSuchNamespaceException` | `07_create_dg_namespaces_prod.sh` has not created the taxi leaves |
| `ImagePullBackOff` | `harbor-pull` secret missing from the `spark` namespace |
