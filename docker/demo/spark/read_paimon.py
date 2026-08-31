"""
Read the paimon_heap demo table from Apache Ozone via Spark + Paimon connector.
Run via: docker compose run --rm spark
"""
from pyspark.sql import SparkSession

MINIO_ENDPOINT = "http://minio:9000"
BUCKET         = "paimon-warehouse"
TABLE_PATH        = f"s3a://{BUCKET}/orders"

spark = (
    SparkSession.builder
    .appName("paimon_heap → MinIO demo")
    .config("spark.hadoop.fs.s3a.impl",                  "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("spark.hadoop.fs.s3a.endpoint",              MINIO_ENDPOINT)
    .config("spark.hadoop.fs.s3a.access.key",            "testuser")
    .config("spark.hadoop.fs.s3a.secret.key",            "testkey123")
    .config("spark.hadoop.fs.s3a.path.style.access",     "true")
    .config("spark.hadoop.fs.s3a.connection.ssl.enabled","false")
    .config("spark.hadoop.fs.s3a.endpoint.region",       "us-east-1")
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)
spark.sparkContext.setLogLevel("WARN")

print()
print("=" * 62)
print("  paimon_heap demo — reading from MinIO")
print(f"  path: {TABLE_PATH}")
print("=" * 62)

df = spark.read.format("paimon").option("path", TABLE_PATH).load()

df.show(truncate=False)
print(f"Total rows: {df.count()}")
print()
