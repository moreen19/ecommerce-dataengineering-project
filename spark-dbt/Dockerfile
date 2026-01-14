FROM apache/spark:3.4.3-python3

###############################################
# 1. Switch to root for privileged operations
###############################################
USER root

# Create Spark JARs directory
RUN mkdir -p /opt/spark/jars

# Copy JARs (Iceberg, Glue, Hadoop AWS, AWS SDK, etc.)
COPY spark-dbt/jars/* /opt/spark/jars/

# Copy Spark configuration
COPY spark-dbt/spark-defaults.conf /opt/spark/conf/spark-defaults.conf

# Fix Spark log directory permissions
RUN mkdir -p /opt/spark/logs && \
    chown -R spark:spark /opt/spark/logs

###############################################
# 2. Install dbt + dependencies
###############################################
# Install system library needed for SASL headers
RUN apt-get update && apt-get install -y git libsasl2-dev libsasl2-modules libsasl2-modules-gssapi-mit gcc g++

# Update pip install block
RUN pip install --no-cache-dir \
    "dbt-core==1.5.3" \
    "dbt-spark[PyHive]==1.5.3" \
    "pyspark==3.4.3" \
    "pure-sasl" \
    "thrift-sasl==0.4.3" \
    "PyHive" \
    "thrift==0.16.0" \
    "boto3" \
    "pyathena" \
    "pandas==2.0.3" 
    

###############################################
# 3. Create dbt project directory (mounted at runtime)
###############################################
RUN mkdir -p /opt/dbt/ecommerce_analytics

###############################################
# 4. (Optional) Iceberg + Glue env hints
#    Spark Thrift Server still reads configs from command-line
###############################################
ENV SPARK_SQL_EXTENSIONS="org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions"
ENV SPARK_SQL_CATALOG_GLUE="org.apache.iceberg.spark.SparkCatalog"
ENV SPARK_SQL_CATALOG_GLUE_CATALOG_IMPL="org.apache.iceberg.aws.glue.GlueCatalog"
ENV SPARK_SQL_CATALOG_GLUE_WAREHOUSE="s3://de-ecommerce-lake-2025-moeandy/"
ENV SPARK_SQL_CATALOG_GLUE_IO_IMPL="org.apache.iceberg.aws.s3.S3FileIO"
ENV SPARK_SQL_CATALOG_GLUE_REGION="us-east-2"


RUN chmod 644 /opt/spark/conf/spark-defaults.conf

###############################################
# 5. Default user back to spark
###############################################
USER spark

# Do NOT override ENTRYPOINT here.
# The service command in docker-compose will start the Thrift Server.
