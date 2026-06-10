from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("test-connections") \
    .config("spark.jars.packages",
        "org.postgresql:postgresql:42.7.3,"
        "com.oracle.database.jdbc:ojdbc11:23.3.0.23.09,"
        "org.apache.hadoop:hadoop-aws:3.3.4,"
        "com.amazonaws:aws-java-sdk-bundle:1.12.262") \
    .config("spark.hadoop.fs.s3a.endpoint", "http://minio:9000") \
    .config("spark.hadoop.fs.s3a.access.key", "minioadmin") \
    .config("spark.hadoop.fs.s3a.secret.key", "minioadmin123") \
    .config("spark.hadoop.fs.s3a.path.style.access", "true") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .getOrCreate()

# Test PostgreSQL
df_pg = spark.read.format("jdbc") \
    .option("url", "jdbc:postgresql://postgres:5432/banking") \
    .option("dbtable", "customers") \
    .option("user", "postgres") \
    .option("password", "postgres123") \
    .option("driver", "org.postgresql.Driver") \
    .load()
print(f"PostgreSQL customers: {df_pg.count()} rows")
df_pg.show(3)

# Test Oracle
df_ora = spark.read.format("jdbc") \
    .option("url", "jdbc:oracle:thin:@//oracle-xe:1521/XEPDB1") \
    .option("dbtable", "bank_accounts") \
    .option("user", "corebanking") \
    .option("password", "oracle123") \
    .option("driver", "oracle.jdbc.OracleDriver") \
    .load()
print(f"Oracle bank_accounts: {df_ora.count()} rows")
df_ora.show(3)

# Test ghi lên MinIO
df_pg.write.mode("overwrite").parquet("s3a://lakehouse/test/customers")
print("Ghi Parquet lên MinIO: OK")

spark.stop()
