# dags/dag_daily_simulation.py
"""
DAG daily_simulation — driver daily DUY NHAT cua he thong.

Luong khep kin moi ngay:
    simulate  ->  bronze_incremental  ->  silver  ->  gold  ->  verify

- simulate: docker exec lakehouse-data-simulator chay generate_data.py --mode daily
  --date {{ ds }} (execution_date = ngay mo phong). Sinh giao dich moi + KH moi +
  doi segment + NPL vao Oracle/Postgres (CDC created_at/updated_at = gio thuc).
- bronze_incremental: chi upsert record co updated_at/created_at > last_run_time
  (KHONG full_snapshot) -> dung that nhanh incremental.
- silver -> gold -> verify: chay lai transform, mart snapshot_date tu khop {{ ds }}.

catchup=True + max_active_runs=1: Airflow backfill TUAN TU tu start_date toi nay,
moi ngay 1 run, dam bao data nap dung thu tu (gold as_of=auto bam theo ngay).

LUU Y van hanh: pause cac DAG bronze_ingestion / silver_transform / gold_mart khi
bat DAG nay (chung chia se bronze.audit_log -> last_run_time; chay doi lam lech cua
so incremental). Giu chung cho chay tay / bootstrap.
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "lakehouse-spark-master"
SIM_CONTAINER   = "lakehouse-data-simulator"

SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
    "org.postgresql:postgresql:42.7.3"
)


def spark_job(layer: str, job: str) -> str:
    return (f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 --packages '{SPARK_PACKAGES}' "
            f"/opt/spark/jobs/{layer}/{job}")


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="daily_simulation",
    default_args=default_args,
    description="Simulate nguon -> bronze incremental -> silver -> gold (daily, end-to-end)",
    start_date=datetime(2026, 6, 8),          # gan day -> catchup chi bu ~1 ngay
    schedule_interval="0 1 * * *",            # 1 AM
    catchup=True,
    max_active_runs=1,                        # tuan tu, dung thu tu ngay
    tags=["daily", "simulation", "incremental"],
) as dag:

    simulate = BashOperator(
        task_id="simulate",
        bash_command=(f"docker exec {SIM_CONTAINER} "
                      f"python /app/scripts/generate_data.py --mode daily --date {{{{ ds }}}}"),
    )
    bronze_incremental = BashOperator(
        task_id="bronze_incremental",
        bash_command=spark_job("bronze", "incremental_load.py"),
    )
    silver = BashOperator(task_id="silver", bash_command=spark_job("silver", "run_silver.py"))
    gold   = BashOperator(task_id="gold",   bash_command=spark_job("gold", "run_gold.py"))
    verify = BashOperator(task_id="verify", bash_command=spark_job("gold", "verify_gold.py"))

    simulate >> bronze_incremental >> silver >> gold >> verify
