from datetime import datetime, timedelta, timezone

from airflow.sdk import DAG
from airflow.providers.standard.operators.bash import BashOperator


DBT = "/opt/dbt-venv/bin/dbt"
PYTHON = "/opt/dbt-venv/bin/python"
PROJECT = "/opt/airflow/project/dbt"
OPTIONS = f"--project-dir {PROJECT} --profiles-dir {PROJECT}"


with DAG(
    dag_id="pharma_analytics_pipeline",
    description="Check CDC catch-up, validate data, and build analytics.",
    start_date=datetime(2026, 10, 13, tzinfo=timezone.utc),
    schedule="*/15 * * * *",
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "anisha",
        "retries": 1,
        "retry_delay": timedelta(seconds=30),
        "execution_timeout": timedelta(minutes=10),
    },
    tags=["pharma", "cdc", "dbt"],
) as dag:

    check_cdc_catchup = BashOperator(
        task_id="check_cdc_catchup",
        bash_command=(
            f"{PYTHON} /opt/airflow/dags/check_cdc_catchup.py"
        ),
    )

    build_staging = BashOperator(
        task_id="build_staging",
        bash_command=f"{DBT} run {OPTIONS} --select path:models/staging",
    )

    validate_staging = BashOperator(
        task_id="validate_staging",
        bash_command=(
            f"{DBT} test {OPTIONS} "
            "--select path:models/staging "
            "--indirect-selection cautious"
        ),
    )

    snapshot_suppliers = BashOperator(
        task_id="snapshot_suppliers",
        bash_command=f"{DBT} snapshot {OPTIONS} --select snap_suppliers",
    )

    build_marts = BashOperator(
        task_id="build_marts",
        bash_command=f"{DBT} run {OPTIONS} --select path:models/marts",
    )

    validate_all = BashOperator(
        task_id="validate_all",
        bash_command=f"{DBT} test {OPTIONS}",
    )

    (
        check_cdc_catchup
        >> build_staging
        >> validate_staging
        >> snapshot_suppliers
        >> build_marts
        >> validate_all
    )