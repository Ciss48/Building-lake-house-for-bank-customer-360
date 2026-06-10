# dags/dag_bronze_ingestion.py
"""
DAG Bronze Ingestion.

Cơ chế: Airflow KHÔNG chạy spark-submit nội bộ (image Airflow không có Java/Spark).
Thay vào đó dùng BashOperator + `docker exec` vào container lakehouse-spark-master
đang chạy (mount /var/run/docker.sock vào Airflow). Đây là cách nhẹ và đúng với
lệnh test thủ công đã chạy ổn ở Phase 1/2.

Flow:
    full_snapshot  ->  incremental_load  ->  quality_check
- full_snapshot   : idempotent, đảm bảo mọi bảng đều có data (chạy lại không sao)
- incremental_load: upsert record mới theo updated_at/created_at
- quality_check   : verify_bronze.py — đếm row count, fail nếu bảng trống

Schedule 2:00 AM mỗi ngày, retry 2 lần cách nhau 5 phút.
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
    """Sinh lệnh docker exec spark-submit cho 1 job Bronze."""
    return (
        f"docker exec {SPARK_CONTAINER} /opt/spark/bin/spark-submit "
        f"--master spark://spark-master:7077 "
        f"--packages '{SPARK_PACKAGES}' "
        f"/opt/spark/jobs/bronze/{job_relpath} {extra_args}"
    ).strip()


default_args = {
    "owner":            "lakehouse",
    "depends_on_past":  False,
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "email_on_failure": False,
}

with DAG(
    dag_id            = "bronze_ingestion",
    default_args      = default_args,
    description       = "Daily ingestion tu Oracle + PostgreSQL vao Bronze Iceberg",
    start_date        = datetime(2024, 11, 1),
    schedule_interval = "0 2 * * *",
    catchup           = False,
    tags              = ["bronze", "ingestion"],
) as dag:

    full_snapshot = BashOperator(
        task_id      = "full_snapshot",
        bash_command = spark_submit_cmd("full_snapshot.py"),
    )

    incremental_load = BashOperator(
        task_id      = "incremental_load",
        bash_command = spark_submit_cmd("incremental_load.py"),
    )

    quality_check = BashOperator(
        task_id      = "quality_check",
        bash_command = spark_submit_cmd("verify_bronze.py"),
    )

    full_snapshot >> incremental_load >> quality_check
