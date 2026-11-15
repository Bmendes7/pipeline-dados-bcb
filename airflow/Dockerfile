FROM apache/airflow:3.4.0

COPY requirements.txt /tmp/requirements.txt

RUN pip install --no-cache-dir "apache-airflow==${AIRFLOW_VERSION}" -r /tmp/requirements.txt

RUN python -m venv /home/airflow/dbt_venv \
&& /home/airflow/dbt_venv/bin/pip install --no-cache-dir dbt-bigquery==1.12.2 dbt-core==1.12.7