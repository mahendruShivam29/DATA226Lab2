"""
A basic dbt DAG that runs seed → run → test → snapshot via BashOperator.
"""

from pendulum import datetime

from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator


DBT_PROJECT_DIR = "/opt/airflow/dbt"           # mounted inside the container
DBT_BIN         = "/home/airflow/.local/bin/dbt"  # same as you used before


# ------------------------------------------------------------------
# Build a clean, all-string environment dict for dbt
# ------------------------------------------------------------------
def _dbt_env(conn_id: str = "snowflake_conn") -> dict:
    """
    Read the Airflow connection and return only non-None, str-typed
    variables so BashOperator / Popen won't choke.
    """
    conn = BaseHook.get_connection(conn_id)

    raw_env = {
        "DBT_USER":       conn.login,
        "DBT_PASSWORD":   conn.password,
        "DBT_ACCOUNT":    conn.extra_dejson.get("account"),
        "DBT_SCHEMA":     conn.schema,
        "DBT_DATABASE":   conn.extra_dejson.get("database"),
        "DBT_ROLE":       conn.extra_dejson.get("role"),
        "DBT_WAREHOUSE":  conn.extra_dejson.get("warehouse"),
        "DBT_TYPE":       "snowflake",
    }

    # drop keys whose value is None and coerce everything to str
    return {k: str(v) for k, v in raw_env.items() if v is not None}


dbt_env = _dbt_env()   # evaluated once at parse-time


# ------------------------------------------------------------------
# DAG definition
# ------------------------------------------------------------------
with DAG(
    dag_id="BuildELT_dbt",
    description="Invoke dbt from Airflow using BashOperator",
    schedule=None,          # Airflow ≥2.8: use `schedule` (or cron string)
    start_date=datetime(2025, 3, 19),
    catchup=False,
) as dag:

    dbt_seed = BashOperator(
        task_id="dbt_seed",
        bash_command=f"{DBT_BIN} seed --profiles-dir {DBT_PROJECT_DIR} "
                     f"--project-dir {DBT_PROJECT_DIR}",
        env=dbt_env,
        append_env=True,    # keep the worker’s existing env as well
    )

    dbt_run = BashOperator(
        task_id="dbt_run",
        bash_command=f"{DBT_BIN} run --profiles-dir {DBT_PROJECT_DIR} "
                     f"--project-dir {DBT_PROJECT_DIR}",
        env=dbt_env,
        append_env=True,
    )

    dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command=f"{DBT_BIN} test --profiles-dir {DBT_PROJECT_DIR} "
                     f"--project-dir {DBT_PROJECT_DIR}",
        env=dbt_env,
        append_env=True,
    )

    dbt_snapshot = BashOperator(
        task_id="dbt_snapshot",
        bash_command=f"{DBT_BIN} snapshot --profiles-dir {DBT_PROJECT_DIR} "
                     f"--project-dir {DBT_PROJECT_DIR}",
        env=dbt_env,
        append_env=True,
    )

    # ── Task-ordering ──────────────────────────────────────────────
    dbt_seed >> dbt_run >> dbt_test >> dbt_snapshot
