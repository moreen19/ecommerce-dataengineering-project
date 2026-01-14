FROM apache/airflow:2.11.0-python3.9

USER root

# 1. Install system dependencies (Java for PySpark JVM, Git for dbt packages)
RUN apt-get update && apt-get install -y --no-install-recommends \
    g++ \
    git-core \
    python3-distutils \
    openjdk-17-jre-headless \
    wget \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# 2. Create clean venv for dbt
RUN python3 -m venv --copies /opt/airflow/dbt_venv
RUN /opt/airflow/dbt_venv/bin/pip install --upgrade pip setuptools wheel

# 3. Install dbt with PySpark client (no local Spark cluster)
RUN /opt/airflow/dbt_venv/bin/pip install \
    "dbt-core==1.5.3" \
    "dbt-spark[pyspark]==1.5.3" \
    "pyspark==3.4.3" \
    "boto3"

# 4. Environment variables for dbt
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV DBT_VENV=/opt/airflow/dbt_venv/bin
ENV RE2_SKIP_BUILD=1

# 5. Copy project code
COPY generator /opt/airflow/generator
COPY ml /opt/airflow/ml
COPY dbt/ecommerce_analytics /opt/airflow/dbt_project/ecommerce_analytics
COPY dags /opt/airflow/dags
COPY dags/requirements.txt /requirements.txt

RUN chown -R airflow:0 /opt/airflow /requirements.txt

USER airflow

# 6. Install Python dependencies for Airflow/DAGs
RUN python3 -m pip install --upgrade pip
RUN python3 -m pip install --no-cache-dir -r /requirements.txt
RUN python3 -m pip install docker


