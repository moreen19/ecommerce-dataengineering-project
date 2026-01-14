import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, current_timestamp
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    TimestampType, IntegerType, BooleanType
)

# --- Configuration (REPLACE THESE WITH YOUR DETAILS) ---
AWS_ACCESS_KEY_ID = os.environ.get("AWS_ACCESS_KEY_ID")
AWS_SECRET_ACCESS_KEY = os.environ.get("AWS_SECRET_ACCESS_KEY")
S3_BUCKET = os.environ.get("S3_BUCKET")

KAFKA_BOOTSTRAP_SERVERS = 'pkc-921jm.us-east-2.aws.confluent.cloud:9092' # Your URL
KAFKA_TOPIC = 'demo.purchases' # Transactional topic

KAFKA_API_KEY = "HWDEAFUVZ26RTOIU"  # Your Service Account API Key
KAFKA_API_SECRET = "cfltBldL2+5lOzAzL6ErOcCeQ9ZkbozUGyT1RJoXzcxU9WhbPLEg0ckufkgLid3Q" # Your Service Account Secret

SILVER_PATH = f"s3a://{S3_BUCKET}/silver/purchases"
CHECKPOINT_PATH = f"s3a://{S3_BUCKET}/checkpoint/purchases"






# --- Schema Definition (Matches the generator's Purchase model) ---
purchase_schema = StructType([
    StructField("transaction_time", TimestampType(), True),
    StructField("transaction_id", StringType(), True),
    StructField("product_id", StringType(), True),
    StructField("price", DoubleType(), True),
    StructField("quantity", IntegerType(), True),
    StructField("is_member", BooleanType(), True),
    StructField("member_discount", DoubleType(), True),
    StructField("add_supplement", BooleanType(), True),
    StructField("supplement_price", DoubleType(), True),
])

def create_spark_session():
    # ... (code remains the same) ...
    # Configuration includes the necessary S3 access and Kafka packages
    # ...
    return SparkSession.builder \
        .appName("KafkaToDeltaLakeStream") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.1,org.apache.hadoop:hadoop-aws:3.3.4") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("fs.s3a.access.key", AWS_ACCESS_KEY_ID) \
        .config("fs.s3a.secret.key", AWS_SECRET_ACCESS_KEY) \
        .config("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
        .config("fs.s3a.aws.credentials.provider", "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider") \
        .getOrCreate()


if __name__ == "__main__":
    spark = create_spark_session()
    
    # 1. Read from Kafka (Secure SASL/SSL connection)
    kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("subscribe", KAFKA_TOPIC) \
        .option("startingOffsets", "earliest") \
        .option("group.id", "spark-purchase-group") \
        .option("kafka.security.protocol", "SASL_SSL") \
        .option("kafka.sasl.mechanism", "PLAIN") \
        .option("kafka.sasl.jaas.config", 
                f"org.apache.kafka.common.security.plain.PlainLoginModule required username='{KAFKA_API_KEY}' password='{KAFKA_API_SECRET}';") \
        .load()
        
    # 2. Parse and Transform Data
    parsed_stream = kafka_stream.select(col("value").cast("string")).alias("kafka") \
        .withColumn("data", from_json(col("kafka.value"), purchase_schema)) \
        .select("data.*") \
        .withColumn("ingestion_timestamp", current_timestamp()) # Essential for dbt incremental loads
    
    
    # 1️ Add console sink to see messages in real time
    console_query = parsed_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") \
    .writeStream.format("console").option("truncate", False).start()
        
    # 3. Write to Delta Lake in S3
    delta_query = parsed_stream.writeStream \
       .format("delta") \
       .outputMode("append") \
       .partitionBy("product_id") \
       .trigger(processingTime="1 minute") \
       .option("checkpointLocation", CHECKPOINT_PATH) \
       .option("path", SILVER_PATH) \
       .start()
    

    # Wait for 3 minutes then stop both
    import time
    time.sleep(180)
    console_query.stop()
    delta_query.stop()
    spark.stop()
    



