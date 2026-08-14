"""
NYC TLC Yellow Taxi -> bronze -> silver -> gold, on Spark, into Iceberg.

One day of trips per run. This is the pipeline the platform exists to run;
everything else in the project -- the catalog, the registry, the autoscaler,
the Trino endpoint -- is there so that these three tasks can run reliably and
be queried afterwards.

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
resources and its own retry. The aggregation failing does not re-download the
source or re-clean the day; it re-runs the minute that failed. The stages also
have genuinely different shapes -- bronze is driver-heavy and network-bound,
silver is executor-heavy, gold is small -- and one pod cannot be sized
correctly for all three.

Every task is idempotent: each deletes the day it is about to write before
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
from airflow.providers.smtp.notifications.smtp import send_smtp_notification

NAMESPACE = "spark"

# WHO HEARS ABOUT A FAILURE
#
# SmtpNotifier, not `email_on_failure`. The latter goes through
# `airflow.utils.email`, which Airflow has scheduled for deprecation; the
# notifier uses the SMTP provider and the `smtp_default` connection, which is
# the path that will still exist in the next major version.
#
# The connection itself is an environment variable injected from a Kubernetes
# Secret -- see 04_airflow_prod/04_create_smtp_secret_prod.sh. Nothing about
# the mail server is in this file, so pointing alerts somewhere else is a
# change to a Secret and a pod restart, not a DAG edit and a git push.
ALERT_TO = "pongkunworker@gmail.com"

# Only on failure, deliberately.
#
# There is no notifier on success and none on retry. A daily pipeline that
# mails on every run produces 365 messages a year that all say "fine", and the
# one that says otherwise arrives in a folder nobody reads any more. Retries
# are excluded for the same reason: the first attempt failing is normal --
# a Spot node went away, a CloudFront request timed out -- and the run still
# succeeds. What deserves an interruption is the state after retries are
# exhausted.
FAILURE_NOTIFIER = send_smtp_notification(
    to=ALERT_TO,
    subject="[Airflow] {{ dag.dag_id }} failed — {{ ti.task_id }}",
    html_content=(
        "<p><b>{{ ti.task_id }}</b> failed after {{ ti.try_number }} attempt(s).</p>"
        "<p>Processing date: <b>{{ resolve_date(params, data_interval_start) }}</b><br>"
        "Run: {{ run_id }}</p>"
        "<p>The Spark driver keeps its logs — the SparkApplication is not deleted "
        "on failure:<br>"
        "<code>kubectl logs -n spark -l spark-role=driver --tail=100</code></p>"
        '<p><a href="{{ ti.log_url }}">Airflow task log</a></p>'
    ),
)

# Runs are scheduled in Bangkok time, which is where the people reading the
# dashboard are. Airflow takes a DAG's timezone from start_date, so this is
# what makes "0 10 * * *" mean 10:00 local rather than 17:00.
LOCAL_TZ = pendulum.timezone("Asia/Bangkok")

# HOW FAR BACK A SCHEDULED RUN LOOKS
#
# TLC publishes each month's file roughly two months after the month ends, so
# "yesterday" does not exist as source data and never will. A run therefore
# processes the same calendar day three months earlier: far enough back that
# the file is certainly published, close enough that the pipeline is visibly
# moving forward one day at a time.
#
# Backfilling a real date range is a manual trigger with {"date": "..."} per
# day, or `airflow dags backfill` -- not a change to this number.
DATA_LAG_MONTHS = 3

with DAG(
    dag_id="nyc_taxi_medallion",
    description="NYC TLC Yellow Taxi: bronze -> silver -> gold in Iceberg, one day per run",
    # 10:00 Asia/Bangkok, every day.
    #
    # A fixed hour rather than @daily (which is midnight): the run takes 5-10
    # minutes when ng-spot has to start from zero, and a failure at 10:00 is
    # noticed the same morning. A failure at 00:15 is noticed at 09:00 anyway,
    # after a night of nobody looking.
    schedule="0 10 * * *",
    start_date=pendulum.datetime(2025, 1, 1, tz=LOCAL_TZ),
    # catchup=False on purpose. Turning it on would launch a backfill of every
    # day since start_date the moment the DAG is unpaused -- hundreds of
    # concurrent Spark jobs against a four-node cluster.
    catchup=False,
    max_active_runs=1,
    tags=["taxi", "iceberg", "medallion", "spark", "daily"],
    default_args={
        "retries": 1,
        "retry_delay": pendulum.duration(minutes=5),
        # Applies to all three tasks. On a linear DAG only the first failure
        # sends -- the downstream tasks are skipped, not failed, and a skip is
        # not a notification-worthy event.
        "on_failure_callback": [FAILURE_NOTIFIER],
    },
    params={
        # Left empty, the DAG derives the date from its own run. Set it to
        # "2024-01-15" on a manual trigger to load one specific day.
        # Every param below carries a `pattern`, and that is not decoration.
        #
        # These three values are interpolated into the https:// URL the Spark
        # driver fetches its code from. A character that is illegal in a URI --
        # a stray "<sha>" placeholder, a space, a brace -- makes Spark's
        # Utils.resolveURI() throw, silently fall back to treating the whole
        # URL as a LOCAL file path, and glob "file:/opt/spark/work-dir/https:/
        # raw.githubusercontent.com/...". The component "https:" then fails to
        # parse as a Path and the run dies with
        #
        #     URISyntaxException: Expected scheme-specific part at index 6: https:
        #
        # which names neither the parameter nor the URL. A pattern rejects the
        # value at trigger time instead, while the message can still be useful.
        "date": Param(
            "",
            type="string",
            pattern=r"^$|^\d{4}-\d{2}-\d{2}$",
            description="YYYY-MM-DD, or blank to derive from the run",
        ),
        # The image supplies the runtime; the repo below supplies the logic.
        # Both are parameters so a run can be pinned to either.
        "image_tag": Param(
            "v1.0.2",
            type="string",
            pattern=r"^[A-Za-z0-9._-]+$",
            description="a tag in harbor.nopega.net/ice-berg-platform/datapipeline",
        ),
        # A SEPARATE repo from this one, deliberately.
        #
        # Anyone who can push to the repo Airflow git-syncs can make Airflow
        # run whatever they like -- that repo is effectively production
        # configuration. Pipeline logic changes far more often and is reviewed
        # by different people, so it lives somewhere its authors can push to
        # without also holding the keys to the scheduler.
        #
        # The cost is atomicity: a change to silver_clean.py that
        # gold_aggregate.py depends on now spans two repos, and between the two
        # pushes the DAG points at code that does not have it yet. Pin
        # `pipeline_ref` to a SHA for a run that must not move underneath itself.
        "pipeline_repo": Param(
            "nopega/ice-berg-data-pipeline",
            type="string",
            pattern=r"^[A-Za-z0-9._-]+/[A-Za-z0-9._-]+$",
            description="owner/repo on GitHub, and it must be public",
        ),
        "pipeline_ref": Param(
            "refs/heads/main",
            type="string",
            pattern=r"^[A-Za-z0-9._/-]+$",
            description="refs/heads/main, or a commit SHA to pin the run",
        ),
    },
    doc_md=__doc__,
    # A macro rather than a per-task `params` override, because BaseOperator's
    # `params` is not a template field: a Jinja expression assigned to it
    # arrives at the YAML as literal text, and the SparkApplication would be
    # named "taxi-bronze-{{ params.date if..." -- rejected by the API server for
    # illegal characters, several minutes after the run started.
    #
    # `data_interval_start` is the beginning of the period a run COVERS, not
    # when it started. On a daily schedule that is the day itself.
    user_defined_macros={
        "resolve_date": (
            lambda params, data_interval_start: params.get("date")
            or data_interval_start.subtract(months=DATA_LAG_MONTHS).format("YYYY-MM-DD")
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
        doc_md="Download the month's published Parquet, keep this day, land it unchanged.",
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
        doc_md="Aggregate to zone x payment method, pre-joined and pre-computed for Power BI.",
        **common,
    )

    bronze >> silver >> gold
