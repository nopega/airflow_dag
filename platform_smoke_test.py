"""Platform smoke test.

Proves the delivery chain works end to end, separately from any real pipeline:

    git push  ->  git-sync sidecar  ->  DAG processor parses the file
              ->  scheduler queues a task
              ->  KubernetesExecutor creates a task pod
              ->  the task runs and reports back

That chain has several places to fail silently. A wrong `subPath` makes
git-sync clone successfully and find nothing; an empty DAG list then looks
identical to "no DAGs written yet". A missing RBAC permission stops the
scheduler creating pods, and the task simply stays queued. Running this once
after install turns each of those into an obvious pass or fail.

Deliberately has no dependency on Spark, Polaris, S3 or the catalog: if it
fails, the problem is Airflow itself, not the data platform around it. The real
pipeline DAGs come later and can then be debugged assuming this part works.

Not scheduled -- trigger it by hand.
"""

from __future__ import annotations

import os
import socket
from datetime import datetime

from airflow.sdk import dag, task

# `airflow.sdk` is the supported DAG-authoring interface in Airflow 3. The old
# paths (airflow.decorators, airflow.models.dag) still work in 3.2 but are
# deprecated and scheduled for removal, so new DAGs use this one.


@dag(
    dag_id="platform_smoke_test",
    description="Verifies git-sync, the scheduler and KubernetesExecutor task pods.",
    start_date=datetime(2026, 1, 1),
    # Manual trigger only. A smoke test on a schedule produces noise that
    # everyone learns to ignore, which defeats the point of running it.
    schedule=None,
    catchup=False,
    tags=["platform", "smoke-test"],
)
def platform_smoke_test():
    @task
    def report_environment() -> dict:
        """Report where and as what this task is running.

        Printing the pod name and namespace is the cheap way to confirm the
        task really ran in its own pod rather than inside the scheduler --
        the difference between KubernetesExecutor working and silently
        falling back.
        """
        info = {
            "pod": socket.gethostname(),
            "namespace": os.environ.get("AIRFLOW__KUBERNETES__NAMESPACE", "unknown"),
            "executor": os.environ.get("AIRFLOW__CORE__EXECUTOR", "unknown"),
            "airflow_home": os.environ.get("AIRFLOW_HOME", "unknown"),
        }
        for key, value in info.items():
            print(f"{key:14}: {value}")
        return info

    @task
    def check_dag_source(info: dict) -> str:
        """Confirm the DAG was read from the git-sync checkout.

        If DAGs were coming from somewhere else -- an image layer, a stale
        volume -- this file would still run, and the fact that git is not
        actually the source of truth would go unnoticed until someone pushed
        a change that never took effect.
        """
        dags_folder = os.environ.get("AIRFLOW__CORE__DAGS_FOLDER", "")
        this_file = os.path.abspath(__file__)
        print(f"dags folder   : {dags_folder}")
        print(f"this file     : {this_file}")

        if "/repo" not in this_file and "git" not in dags_folder:
            print(
                "WARNING: this file does not look like it came from the git-sync "
                "checkout. DAGs may be served from a stale copy, in which case "
                "pushing to the repository will appear to do nothing."
            )
        else:
            print("source looks like the git-sync checkout -- as expected")

        return f"ran in pod {info['pod']}"

    check_dag_source(report_environment())


platform_smoke_test()
