# dags/dag_silver_transform.py
"""
DAG Silver Transform (Bronze -> Silver).

Cùng cơ chế Bronze DAG: BashOperator + `docker exec` vào lakehouse-spark-master
(image Airflow không có Java/Spark). Mount /var/run/docker.sock đã cấu hình sẵn.

Flow:
    silver_transform  ->  quality_check
- silver_transform : run_silver.py — SCD1/SCD2 dims + cleansed facts (idempotent)
- quality_check    : verify_silver.py — đếm row count + audit log

Schedule 3:00 AM (sau Bronze 2:00 AM), retry 2 lần cách nhau 5 phút.
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


def spark_submit_cmd(job_relpath: str, extra_args: str = "") -> str:
    """Sinh lệnh docker exec spark-submit cho 1 job Silver."""
    return (
        f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 "
        f"--packages '{SPARK_PACKAGES}' "
        f"/opt/spark/jobs/silver/{job_relpath} {extra_args}"
    ).strip()


default_args = {
    "owner":            "lakehouse",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id            = "silver_transform",
    default_args      = default_args,
    description       = "Bronze -> Silver: SCD1/SCD2 dims + cleansed facts",
    start_date        = datetime(2024, 11, 1),
    schedule_interval = "0 3 * * *",
    catchup           = False,
    tags              = ["silver", "scd"],
) as dag:

    silver_transform = BashOperator(
        task_id      = "silver_transform",
        bash_command = spark_submit_cmd("run_silver.py"),
    )

    quality_check = BashOperator(
        task_id      = "quality_check",
        bash_command = spark_submit_cmd("verify_silver.py"),
    )

    silver_transform >> quality_check
