# dags/dag_gold_mart.py
"""
Airflow DAG: Silver -> Gold. Build mart_customer_360 (+ 2 agg) roi quality_check.
Mirror dag_silver_transform: BashOperator + docker exec lakehouse-spark-master spark-submit.
Schedule 4 AM (sau Silver 3 AM).
"""
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator

SPARK_CONTAINER = "lakehouse-spark-master"
SPARK_PACKAGES = (
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,"
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0,"
    "org.apache.hadoop:hadoop-aws:3.3.4,"
    "com.amazonaws:aws-java-sdk-bundle:1.12.262,"
    "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
    "org.postgresql:postgresql:42.7.3"
)


def submit(job: str) -> str:
    return (f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 --packages '{SPARK_PACKAGES}' "
            f"/opt/spark/jobs/gold/{job}")


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="gold_mart",
    default_args=default_args,
    description="Silver -> Gold: mart_customer_360 + RFM + cross-sell",
    start_date=datetime(2024, 11, 1),
    schedule_interval="0 4 * * *",          # 4 AM, sau silver (3 AM)
    catchup=False,
    tags=["gold", "mart", "rfm"],
) as dag:
    gold_build = BashOperator(task_id="gold_build", bash_command=submit("run_gold.py"))
    quality_check = BashOperator(task_id="quality_check", bash_command=submit("verify_gold.py"))
    gold_build >> quality_check
