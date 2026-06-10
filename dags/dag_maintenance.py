# dags/dag_maintenance.py
"""
Airflow DAG: bao tri Iceberg dinh ky (hang tuan, Chu nhat 5 AM).
Compaction + rewrite manifests + expire snapshots tren cac bang lon (bronze/silver/gold).
Mirror dag_gold_mart: BashOperator + docker exec lakehouse-spark-master spark-submit.
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


def submit(path: str) -> str:
    return (f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
            f"--master spark://spark-master:7077 --packages '{SPARK_PACKAGES}' "
            f"/opt/spark/jobs/{path}")


default_args = {
    "owner": "lakehouse",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id="maintenance",
    default_args=default_args,
    description="Iceberg maintenance: compaction + expire snapshots (weekly)",
    start_date=datetime(2024, 11, 1),
    schedule_interval="0 5 * * 0",          # 5 AM Chu nhat
    catchup=False,
    tags=["maintenance", "ops", "governance"],
) as dag:
    maintain = BashOperator(
        task_id="maintain_tables",
        bash_command=submit("maintenance/maintain_tables.py"),
    )
