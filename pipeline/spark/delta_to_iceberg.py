from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date
import os

spark = (
    SparkSession.builder
    .appName("DeltaToIcebergGold")
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.sql.catalog.glue", "org.apache.iceberg.spark.SparkCatalog")
    .config("spark.sql.catalog.glue.warehouse", "s3://de-ecommerce-lake-2025-moeandy/")
    .config("spark.sql.catalog.glue.catalog-impl", "org.apache.iceberg.aws.glue.GlueCatalog")
    .config("spark.sql.catalog.glue.io-impl", "org.apache.iceberg.aws.s3.S3FileIO")
    .config("spark.sql.catalog.glue.region", "us-east-2")
    .config("aws.region", "us-east-2")
    .config("fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
    .config("fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
    .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
    .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider")
    .getOrCreate()
)

silver_df = spark.read.format("delta").load(
    "s3a://de-ecommerce-lake-2025-moeandy/silver/purchases/"
)

catalog = "glue"
db = "ecommerce_data_lake"
table = "purchases_iceberg"
full_name = f"{catalog}.{db}.{table}"

# Add partition column
silver_df = silver_df.withColumn("purchase_date", to_date("transaction_time"))

# Write logic with fanout enabled
if spark.catalog.tableExists(full_name):
    (
        silver_df
        .writeTo(full_name)
        .option("fanout-enabled", "true")
        .append()
    )
else:
    (
        silver_df
        .writeTo(full_name)
        .using("iceberg")
        .tableProperty("format-version", "2")
        .partitionedBy("purchase_date")
        .option("fanout-enabled", "true")
        .create()
    )

spark.stop()
