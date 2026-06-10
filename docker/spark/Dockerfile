# docker/spark/Dockerfile
# apache/spark + PyYAML (các Spark job đọc config/bronze_tables.yaml ở driver).
# Bake vào image để persist qua mọi lần `docker compose up` (Airflow DAG cũng cần).
FROM apache/spark:3.5.1

USER root
RUN python3 -m pip install --no-cache-dir "pyyaml==6.0.1"
