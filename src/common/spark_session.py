# src/common/spark_session.py
"""
SparkSession chuẩn dùng chung cho toàn project (Iceberg + MinIO + Nessie).
Import hàm này thay vì tạo SparkSession trực tiếp trong từng job.

GHI CHÚ KỸ THUẬT (khác với draft trong task file, đã fix 2 latent bug):

1. URI Nessie = http://nessie:19120/api/v2 (KHÔNG phải /iceberg)
   NessieCatalog (client native trong nessie-spark-extensions) nói chuyện với
   Nessie REST API v2. Endpoint /iceberg là Iceberg-REST (cần RESTCatalog),
   không tương thích với NessieCatalog.

2. io-impl = HadoopFileIO (KHÔNG phải S3FileIO)
   S3FileIO dùng AWS SDK v2 (software.amazon.awssdk) — KHÔNG có trong package
   aws-java-sdk-bundle:1.12.262 (SDK v1). HadoopFileIO tái dùng đúng S3A stack
   (hadoop-aws + fs.s3a.*) đã chạy ổn ở Phase 1.

3. spark.jars.packages đặt ở đây CHỈ để tài liệu hoá. Khi chạy bằng spark-submit
   PHẢI truyền --packages trên CLI (xem SPARK_PACKAGES bên dưới) vì Ivy resolve
   jar lúc launch JVM, trước khi code Python chạy.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType


# Dùng cho cả --packages của spark-submit (CLI) lẫn config builder.
SPARK_PACKAGES = ",".join([
    "org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2",
    "org.projectnessie.nessie-integrations:nessie-spark-extensions-3.5_2.12:0.79.0",
    "org.apache.hadoop:hadoop-aws:3.3.4",
    "com.amazonaws:aws-java-sdk-bundle:1.12.262",
    "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09",
    "org.postgresql:postgresql:42.7.3",
])


def get_spark_session(app_name: str) -> SparkSession:
    """Tạo SparkSession chuẩn với Iceberg + MinIO + Nessie."""
    minio_endpoint   = os.getenv("MINIO_ENDPOINT",       "http://minio:9000")
    minio_access_key = os.getenv("AWS_ACCESS_KEY_ID",    "minioadmin")
    minio_secret_key = os.getenv("AWS_SECRET_ACCESS_KEY", "minioadmin123")
    # Mặc định trỏ /api/v2 (xem ghi chú #1). Có thể override qua ENV nếu cần.
    nessie_uri       = os.getenv("NESSIE_URI",            "http://nessie:19120/api/v2")
    warehouse        = os.getenv("ICEBERG_WAREHOUSE",     "s3a://lakehouse/warehouse")
    spark_master     = os.getenv("SPARK_MASTER_URL",      "spark://spark-master:7077")

    return (
        SparkSession.builder
        .appName(app_name)
        .master(spark_master)
        .config("spark.jars.packages", SPARK_PACKAGES)

        # Iceberg + Nessie SQL extensions
        .config("spark.sql.extensions",
                "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions,"
                "org.projectnessie.spark.extensions.NessieSparkSessionExtensions")

        # Nessie catalog
        .config("spark.sql.catalog.nessie",              "org.apache.iceberg.spark.SparkCatalog")
        .config("spark.sql.catalog.nessie.catalog-impl", "org.apache.iceberg.nessie.NessieCatalog")
        .config("spark.sql.catalog.nessie.uri",           nessie_uri)
        .config("spark.sql.catalog.nessie.ref",           "main")
        .config("spark.sql.catalog.nessie.warehouse",     warehouse)
        # HadoopFileIO — tái dùng S3A stack (xem ghi chú #2)
        .config("spark.sql.catalog.nessie.io-impl",       "org.apache.iceberg.hadoop.HadoopFileIO")

        # MinIO S3-compatible (dùng bởi HadoopFileIO/S3A)
        .config("spark.hadoop.fs.s3a.endpoint",           minio_endpoint)
        .config("spark.hadoop.fs.s3a.access.key",         minio_access_key)
        .config("spark.hadoop.fs.s3a.secret.key",         minio_secret_key)
        .config("spark.hadoop.fs.s3a.path.style.access",  "true")
        .config("spark.hadoop.fs.s3a.impl",               "org.apache.hadoop.fs.s3a.S3AFileSystem")
        .config("spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
        .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

        # Performance tuning cho 16GB RAM
        .config("spark.driver.memory",   "2g")
        .config("spark.executor.memory", "2g")
        .config("spark.executor.cores",  "2")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
        .config("spark.sql.shuffle.partitions", "20")

        .getOrCreate()
    )


def get_jdbc_options(source_config: dict) -> dict:
    """JDBC options cho spark.read.format('jdbc'). Password đọc từ ENV trong container."""
    password = os.getenv(source_config["password_env"])
    if not password:
        raise RuntimeError(
            f"Biến môi trường {source_config['password_env']} chưa được set trong container Spark. "
            f"Kiểm tra environment của spark-master/spark-worker trong docker-compose.yml."
        )
    return {
        "url":      source_config["jdbc_url"],
        "driver":   source_config["driver"],
        "user":     source_config["user"],
        "password": password,
    }


def align_to_table(spark: SparkSession, df, table_fqn: str):
    """
    Căn chỉnh DataFrame nguồn cho khớp schema bảng Iceberg đích trước khi ghi.

    Lý do: Oracle JDBC trả tên cột VIẾT HOA (BRANCH_ID) và kiểu NUMBER → Spark Decimal,
    trong khi bảng Bronze khai báo cột chữ thường + DOUBLE. Hàm này:
      - map cột theo tên (không phân biệt hoa/thường)
      - cast về đúng kiểu của cột đích
      - cột đích thiếu trong nguồn → NULL
      - bỏ qua cột thừa ở nguồn (không có trong bảng đích)
    Nhờ đó writeTo()/MERGE INTO không lỗi schema/type mismatch.
    """
    target_schema: StructType = spark.table(table_fqn).schema
    df_cols = {c.lower(): c for c in df.columns}

    selected = []
    for field in target_schema.fields:
        src_col = df_cols.get(field.name.lower())
        if src_col is None:
            selected.append(F.lit(None).cast(field.dataType).alias(field.name))
        else:
            selected.append(F.col(f"`{src_col}`").cast(field.dataType).alias(field.name))

    return df.select(*selected)
