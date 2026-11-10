FROM apache/airflow:2.10.0

USER airflow
ARG AIRFLOW_VERSION=2.10.0
ARG PYTHON_VERSION=3.12
RUN pip install --no-cache-dir \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt" \
    "confluent-kafka[avro]" \
    redis \
    fastavro \
    apache-airflow-providers-databricks
