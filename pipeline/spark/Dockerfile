# Base image with Java, Python, and Spark installed
FROM apache/spark:3.4.3-python3

# Set the working directory
WORKDIR /opt/spark/work-dir

COPY spark_deps/jars /opt/spark/jars-extra
COPY pipeline/spark/requirements.txt .



# 10 | # Temporarily switch to root to install system-wide packages
USER root 

RUN mkdir -p /home/spark/.ivy2 && \
chown -R spark:spark /home/spark/.ivy2 

# 11 | # Install Python dependencies needed for the Spark job
RUN pip install --no-cache-dir -r requirements.txt

# 12 | # Switch back to the default non-root user (like 'spark') for security, if defined by base image
USER spark

# Copy the consumer script
COPY pipeline/spark/streaming_consumer.py .
COPY pipeline/spark/delta_to_iceberg.py .


# Environment variables are set via docker-compose (for keys/bucket)