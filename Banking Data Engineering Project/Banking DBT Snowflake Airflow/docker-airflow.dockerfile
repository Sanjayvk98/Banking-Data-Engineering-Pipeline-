# Dockerfile-airflow
FROM apache/airflow:2.9.3

# Switch to airflow user first
USER airflow

# Install dbt packages
RUN pip install --no-cache-dir dbt-core==1.7.14 dbt-snowflake==1.7.14