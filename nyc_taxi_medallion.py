"""
NYC TLC Yellow Taxi -> bronze -> silver -> gold, on Spark, into Iceberg.

This is the pipeline the platform exists to run. Everything else in the repo --
the catalog, the registry, the autoscaler, the Trino endpoint -- is here so
that these three tasks can run reliably and be queried afterwards.

HOW A TASK BECOMES A SPARK JOB
--------------------------------
    SparkKubernetesOperator (this file, in an Airflow worker pod)
        creates a SparkApplication object from spark/<stage>.yaml
            the Spark Operator sees the object and creates a driver pod
                the driver downloads its job from github over https
                    the driver requests executors, which land on Spot
                        the operator reports status back
    the operator streams the driver's log into this task and fails with it

Airflow never creates a pod itself. Its ServiceAccount can create
SparkApplications and read pods, and deliberately cannot create pods -- so a
compromised DAG can ask for a Spark job but cannot open a shell in the cluster.
(`kubectl auth can-i create pods -n spark --as=system:serviceaccount:airflow:airflow-worker`
answers no, and 06_spark_prod/01_create_namespace_and_sa_prod.sh checks it.)

WHY THREE TASKS AND NOT ONE
-----------------------------
Each stage is a separate SparkApplication with its own driver, its own
resources and its own retry. The aggregation failing does not re-download 60 MB
or re-clean three million rows; it re-runs the two minutes that failed. The
stages also have genuinely different shapes -- bronze is driver-heavy and
network-bound, silver is executor-heavy, gold is small -- and one pod cannot be
sized correctly for all three.

Every task is idempotent: each deletes the month it is about to write before
writing it, so a retry after a partial write leaves the same table as a clean
first run rather than duplicated rows.
"""

from __future__ import annotations

import pendulum
from airflow.models.dag import DAG
from airflow.models.param import Param
from airflow.providers.cncf.kubernetes.operators.spark_kubernetes import (
    SparkKubernetesOperator,
)

NAMESPACE = "spark"

# TLC publishes each month roughly two months later. A DAG run for March is
# therefore about January's data. Getting this wrong produces a 403 from
# CloudFront that reads like a permissions problem.
PUBLISH_LAG_MONTHS = 2

with DAG(
    dag_id="nyc_taxi_medallion",
    description="NYC TLC Yellow Taxi: bronze -> silver -> gold in Iceberg",
    schedule="@monthly",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    # catchup=False on purpose. Turning it on would launch a backfill of every
    # month since start_date the moment the DAG is unpaused -- dozens of
    # concurrent Spark jobs against a four-node cluster. Backfill a range
    # deliberately instead, with `airflow dags backfill`.
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "iceberg", "medallion", "spark"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
    },
    params={
        # Left empty, the DAG derives the month from its own run. Set it to
        # "2024-01" on a manual trigger to load one specific month.
        "month": Param("", type="string", description="YYYY-MM, or blank to derive from the run"),
        # The image supplies the runtime; the repo below supplies the logic.
        # Both are parameters so a run can be pinned to either.
        "image_tag": Param("v1.0.2", type="string"),
        # A SEPARATE repo from this one, deliberately.
        #
        # Anyone who can push to the repo Airflow git-syncs can make Airflow
        # run whatever they like -- that repo is effectively production
        # configuration. Pipeline logic changes far more often and is reviewed
        # by different people, so it lives somewhere its authors can push to
        # without also holding the keys to the scheduler.
        #
        # The cost is atomicity: a change to silver_clean.py that gold_aggregate.py
        # depends on now spans two repos, and between the two pushes the DAG
        # points at code that does not have it yet. Pin `pipeline_ref` to a SHA
        # for a run that must not move underneath itself.
        "pipeline_repo": Param("nopega/data-pipeline", type="string"),
        "pipeline_ref": Param("refs/heads/main", type="string"),
    },
    doc_md=__doc__,
    # A macro rather than a per-task `params` override, because BaseOperator's
    # `params` is not a template field: a Jinja expression assigned to it
    # arrives at the YAML as literal text, and the SparkApplication would be
    # named "taxi-bronze-{{ params.month if..." -- rejected by the API server
    # for illegal characters, several minutes after the run started.
    #
    # `data_interval_start` is the beginning of the period a run COVERS, not
    # when it started. On a monthly schedule that is the first of the month,
    # which is what makes subtract() land on a clean month boundary.
    user_defined_macros={
        "resolve_month": (
            lambda params, data_interval_start: params.get("month")
            or data_interval_start.subtract(months=PUBLISH_LAG_MONTHS).format("YYYY-MM")
        ),
    },
) as dag:
    common = dict(
        namespace=NAMESPACE,
        # The operator waits for the SparkApplication to finish and streams the
        # driver's stdout into the Airflow task log, so the [1/5]... progress
        # lines the jobs print are visible in the UI without kubectl.
        get_logs=True,
        # Show the pod's Kubernetes events when it fails. Without this, an
        # ImagePullBackOff or a failed scheduling decision appears in Airflow
        # as a bare non-zero exit.
        log_events_on_failure=True,
        # Keep the driver pod after a FAILED run so its log survives long
        # enough to read. Successful runs are cleaned up by the operator's own
        # timeToLiveSeconds in the YAML.
        delete_on_termination=False,
        do_xcom_push=False,
    )

    bronze = SparkKubernetesOperator(
        task_id="bronze_ingest",
        application_file="spark/bronze_ingest.yaml",
        doc_md="Download the published Parquet and land it unchanged, plus ingest metadata.",
        **common,
    )

    silver = SparkKubernetesOperator(
        task_id="silver_clean",
        application_file="spark/silver_clean.yaml",
        doc_md="Apply and COUNT every quality rule, de-duplicate, derive per-trip measures.",
        **common,
    )

    gold = SparkKubernetesOperator(
        task_id="gold_aggregate",
        application_file="spark/gold_aggregate.yaml",
        doc_md="Aggregate to day x zone x payment method, pre-joined and pre-computed for Power BI.",
        **common,
    )

    bronze >> silver >> gold
