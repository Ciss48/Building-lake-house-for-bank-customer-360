# docker/simulator/Dockerfile
# Container chuyen dung cho data simulator. Airflow `docker exec` vao day chay
# generate_data.py --mode daily (giong pattern docker exec spark-master).
# scripts/ + config/ mount qua volume (khong COPY) de sua nong.
FROM python:3.12-slim

RUN pip install --no-cache-dir psycopg2-binary oracledb pyyaml python-dotenv

WORKDIR /app
CMD ["sleep", "infinity"]
