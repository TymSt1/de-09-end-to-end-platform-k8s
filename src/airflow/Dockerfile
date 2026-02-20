FROM apache/airflow:3.1.7-python3.12

USER root
RUN apt-get update && apt-get install -y curl && \
    curl -LO "https://dl.k8s.io/release/v1.32.0/bin/linux/amd64/kubectl" && \
    chmod +x kubectl && mv kubectl /usr/local/bin/ && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

USER airflow
COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

USER root
COPY dbt/ /opt/airflow/dbt/
COPY dags/ /opt/airflow/dags/
RUN mkdir -p /opt/airflow/dbt/logs /opt/airflow/dbt/target && \
    chown -R airflow:root /opt/airflow/dbt /opt/airflow/dags

USER airflow