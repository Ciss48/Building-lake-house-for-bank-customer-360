# docker/airflow/Dockerfile
# Airflow image + docker CLI tĩnh (để BashOperator chạy `docker exec` vào spark-master).
# KHÔNG cài Java/Spark trong Airflow — Spark chạy ở container spark-master riêng.
FROM apache/airflow:2.9.3-python3.11

USER root

# Cài docker CLI client (bản static, ~chỉ client, không phải full engine)
ARG DOCKER_CLI_VERSION=24.0.7
RUN curl -fsSL "https://download.docker.com/linux/static/stable/x86_64/docker-${DOCKER_CLI_VERSION}.tgz" \
      -o /tmp/docker.tgz \
 && tar -xzf /tmp/docker.tgz --strip-components=1 -C /usr/local/bin docker/docker \
 && rm /tmp/docker.tgz \
 && docker --version

USER airflow
