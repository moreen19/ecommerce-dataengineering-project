from airflow.decorators import dag
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import timedelta
import pendulum
import os

ML_SCRIPT_PATH = '/opt/airflow/ml/anomaly_detection.py'

# Get values from the environment (Docker Compose)
AWS_ID = os.getenv("AWS_ACCESS_KEY_ID", "")
AWS_SECRET = os.getenv("AWS_SECRET_ACCESS_KEY", "")
AWS_REG = os.getenv("AWS_REGION", "us-east-2")

# -----------------------------------------------------
# RAW DBT COMMAND — NO INDENTATION ALLOWED
# -----------------------------------------------------
DBT_COMMAND = f"""
set -e
docker exec \
  -e AWS_ACCESS_KEY_ID={AWS_ID} \
  -e AWS_SECRET_ACCESS_KEY={AWS_SECRET} \
  -e AWS_REGION={AWS_REG} \
  spark-dbt bash -lc '
    export PYSPARK_PYTHON=python3
    export PYSPARK_DRIVER_PYTHON=python3
    cd /opt/dbt/ecommerce_analytics
    dbt debug --target dev --profiles-dir /opt/dbt/ecommerce_analytics/profiles
    dbt run --target dev --profiles-dir /opt/dbt/ecommerce_analytics/profiles
  '
"""

# -----------------------------------------------------
# RAW SPARK STREAMING COMMAND
# -----------------------------------------------------
SPARK_STREAMING_CMD = r"""
set -e
docker exec spark-consumer spark-submit \
  --conf spark.ui.enabled=false \
  --packages io.delta:delta-core_2.12:2.4.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.4.3,org.apache.hadoop:hadoop-aws:3.3.4 \
  /opt/spark/work-dir/streaming_consumer.py
"""

# -----------------------------------------------------
# RAW DELTA → ICEBERG COMMAND
# -----------------------------------------------------
DELTA_TO_ICEBERG_CMD = r"""
set -e
docker exec spark-consumer spark-submit \
  --jars /opt/spark/jars-extra/* \
  /opt/spark/work-dir/delta_to_iceberg.py
"""


@dag(
    dag_id="ecommerce_dataops_pipeline",
    start_date=pendulum.datetime(2025, 1, 1, tz="UTC"),
    schedule=None,
    catchup=False,
    tags=["dataops", "ecommerce", "kafka", "spark", "dbt", "ml"],
)
def sales_pipeline():

    start = EmptyOperator(task_id="start")
    join_streams = EmptyOperator(task_id="join_streams")

    produce_sales_stream = BashOperator(
        task_id="produce_sales_stream",
        bash_command="python -m generator.producer",
        cwd="/opt/airflow",
    )

    spark_streaming = BashOperator(
        task_id="run_streaming_consumer",
        bash_command=SPARK_STREAMING_CMD,
        execution_timeout=timedelta(minutes=7),
        retries=0,
    )

    delta_to_iceberg = BashOperator(
        task_id="delta_to_iceberg",
        bash_command=DELTA_TO_ICEBERG_CMD,
        retries=0,
    )

    run_dbt = BashOperator(
        task_id="run_dbt_transformation",
        bash_command=DBT_COMMAND,
    )

    run_anomaly_detection = BashOperator(
        task_id="run_anomaly_detection_model",
        bash_command=f"python {ML_SCRIPT_PATH}",
    )

    ingest_datahub_metadata = BashOperator(
        task_id="ingest_datahub_metadata",
        bash_command="echo 'Metadata ingestion complete.'",
    )

    start >> produce_sales_stream
    start >> spark_streaming

    produce_sales_stream >> join_streams
    spark_streaming >> join_streams

    join_streams >> delta_to_iceberg
    delta_to_iceberg >> run_dbt
    run_dbt >> run_anomaly_detection
    run_anomaly_detection >> ingest_datahub_metadata


sales_pipeline()
