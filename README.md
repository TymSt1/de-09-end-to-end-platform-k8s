# 🚕 End-to-End Data Platform on Kubernetes

A production-style data platform deployed on a local Kubernetes cluster, orchestrating real-time streaming, batch processing, SQL transformations, and interactive analytics. Project 9 of 13 in a [Data Engineering Roadmap](https://github.com/TymSt1/de-roadmap).

## Architecture

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                          Kubernetes (minikube)                              │
│                                                                             │
│  ┌─────────────┐    ┌──────────────┐    ┌──────────────┐                   │
│  │  Producer    │───▶│    Kafka      │───▶│  Consumer    │                   │
│  │ (Python)     │    │  (Strimzi)    │    │ (Python)     │                   │
│  └─────────────┘    └──────────────┘    └──────┬───────┘                   │
│   streaming ns       streaming ns              │                            │
│                                                ▼                            │
│  ┌──────────────────────────────────────────────────────┐                   │
│  │              S3 (LocalStack)                          │                   │
│  │    platform-raw-data/    platform-processed-data/     │                   │
│  │         (Terraform-provisioned)                       │                   │
│  └──────────────┬───────────────────────┬───────────────┘                   │
│     data ns     │                       │                                   │
│                 ▼                        │                                   │
│  ┌──────────────────┐                   │                                   │
│  │   Apache Spark    │───────────────────┘                                  │
│  │  (Batch Process)  │                                                      │
│  └────────┬─────────┘                                                      │
│  processing ns                                                              │
│            ▼                                                                │
│  ┌──────────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │   PostgreSQL      │◀───│  dbt (6 models│◀───│   Airflow     │              │
│  │   (Warehouse)     │    │  18 tests)    │    │ (7-task DAG)  │              │
│  └────────┬─────────┘    └──────────────┘    └──────────────┘              │
│   data ns │               orchestration ns    orchestration ns              │
│           ▼                                                                 │
│  ┌──────────────────┐    ┌──────────────┐    ┌──────────────┐              │
│  │    Streamlit      │    │  Prometheus   │    │   Grafana     │              │
│  │   (Dashboard)     │    │  (Metrics)    │    │ (Dashboards)  │              │
│  └──────────────────┘    └──────────────┘    └──────────────┘              │
│   serving ns              monitoring ns       monitoring ns                 │
└─────────────────────────────────────────────────────────────────────────────┘
```

## Data Flow

1. **Streaming Ingestion**: Python producer generates synthetic NYC taxi ride events at 10 events/sec into Kafka (Strimzi, KRaft mode)
2. **Raw Storage**: Python consumer batches events and writes Parquet files to S3 (LocalStack) with date partitioning
3. **Batch Processing**: Apache Spark reads raw Parquet from S3, cleans, enriches (fare/mile, tip %, rush hour flags), and writes processed data back to S3
4. **Staging Load**: Processed Parquet loaded into PostgreSQL staging schema with deduplication (ON CONFLICT DO NOTHING)
5. **Transformation**: dbt transforms staging data through staging → intermediate → marts layers (6 models, 18 tests)
6. **Analytics**: Streamlit dashboard queries dbt marts for real-time visualizations
7. **Orchestration**: Airflow DAG (KubernetesExecutor) coordinates the full pipeline as a 7-task workflow
8. **Monitoring**: Prometheus scrapes cluster metrics, Grafana displays pod health, resource usage, and service status

## Pipeline DAG

```
start_streaming → accumulate_data → stop_streaming → spark_processing → load_staging → dbt_run → dbt_test
```

Each DAG run accumulates new ride data — ride counts grow across runs while aggregation tables are refreshed.

## Tech Stack

| Component | Technology | Namespace |
|-----------|------------|-----------|
| Streaming | Apache Kafka (Strimzi Operator, KRaft) | streaming |
| Storage | S3 via LocalStack (Terraform-provisioned) | data |
| Batch Processing | Apache Spark 3.5.4 (PySpark) | processing |
| Warehouse | PostgreSQL 16 | data |
| Transformation | dbt-core 1.11.6 (dbt-postgres) | orchestration |
| Orchestration | Apache Airflow 3.1.7 (KubernetesExecutor) | orchestration |
| Dashboard | Streamlit + Plotly | serving |
| Monitoring | Prometheus + Grafana + kube-state-metrics | monitoring |
| Infrastructure | Terraform (LocalStack S3 buckets) | - |
| CI/CD | GitHub Actions | - |
| Container Orchestration | Kubernetes (minikube) | - |
| Package Management | Helm (Strimzi, Airflow) | - |

## Kubernetes Namespaces

| Namespace | Purpose | Services |
|-----------|---------|----------|
| data | Storage layer | PostgreSQL, LocalStack |
| streaming | Event streaming | Kafka (Strimzi), Producer, Consumer |
| processing | Batch compute | Spark Jobs |
| orchestration | Pipeline coordination | Airflow (api-server, scheduler, dag-processor, triggerer, postgresql) |
| serving | Analytics | Streamlit |
| monitoring | Observability | Prometheus, Grafana, kube-state-metrics |

## dbt Models

```
staging/
  stg_rides.sql          -- Source rides from Spark output (view)

intermediate/
  int_rides_enriched.sql -- Business categorization: distance, fare, tip categories, borough flow (view)

marts/
  fact_rides.sql         -- All enriched rides (table)
  dim_zones.sql          -- Zone performance: revenue, tips, rankings (table)
  fact_hourly_summary.sql-- Hourly metrics: trip counts, revenue, rush hour (table)
  dim_payment_analysis.sql-- Payment type breakdown (table)
```

18 dbt tests: not_null, unique, accepted_values (distance/fare/tip categories), plus a custom test for positive revenue.

## Project Structure

```
de-09-end-to-end-platform-k8s/
├── .github/workflows/
│   └── ci.yml                    # CI/CD: lint, validate, build
├── k8s/
│   ├── namespaces.yaml
│   ├── data/                     # PostgreSQL, LocalStack manifests
│   ├── streaming/                # Kafka cluster, producer, consumer
│   ├── processing/               # Spark batch job
│   ├── orchestration/            # Airflow Helm values, RBAC
│   ├── serving/                  # Streamlit deployment
│   └── monitoring/               # Prometheus, Grafana, kube-state-metrics
├── terraform/
│   └── main.tf                   # S3 bucket provisioning
├── src/
│   ├── producer/                 # Kafka event producer (Python)
│   ├── consumer/                 # Kafka-to-S3 consumer (Python)
│   ├── spark/                    # PySpark batch processing
│   ├── streamlit/                # Dashboard app
│   ├── dags/                     # Airflow DAG source
│   └── airflow/                  # Custom Airflow image (dbt, kubectl, DAGs)
│       ├── Dockerfile
│       ├── requirements.txt
│       ├── dags/
│       └── dbt/                  # Full dbt project
│           ├── dbt_project.yml
│           ├── profiles.yml
│           ├── models/
│           ├── tests/
│           └── macros/
└── README.md
```

## Prerequisites

- Docker Desktop with WSL2
- minikube
- kubectl
- Helm
- Terraform
- AWS CLI (for LocalStack verification)

## Quick Start

### 1. Start the cluster

```bash
minikube start --cpus=4 --memory=12288 --driver=docker
```

### 2. Create namespaces

```bash
kubectl apply -f k8s/namespaces.yaml
```

### 3. Deploy infrastructure

```bash
# PostgreSQL and LocalStack
kubectl apply -f k8s/data/

# Provision S3 buckets
kubectl port-forward svc/localstack -n data 4566:4566 &
cd terraform && terraform init && terraform apply -auto-approve && cd ..

# Kafka (Strimzi)
helm repo add strimzi https://strimzi.io/charts/
helm install strimzi-operator strimzi/strimzi-kafka-operator -n streaming
kubectl apply -f k8s/streaming/kafka-cluster.yaml
```

### 4. Build images (point Docker to minikube)

```bash
eval $(minikube docker-env)  # Linux/Mac
# PowerShell: & minikube -p minikube docker-env --shell powershell | Invoke-Expression

docker build -t taxi-producer:latest src/producer/
docker build -t taxi-consumer:latest src/consumer/
docker build -t taxi-spark:latest src/spark/
docker build -t taxi-streamlit:latest src/streamlit/
docker build -t custom-airflow:v7 src/airflow/
```

### 5. Deploy services

```bash
# Streaming
kubectl apply -f k8s/streaming/producer-deployment.yaml
kubectl apply -f k8s/streaming/consumer-deployment.yaml

# Scale to 0 (Airflow controls these)
kubectl scale deployment/taxi-producer -n streaming --replicas=0
kubectl scale deployment/taxi-consumer -n streaming --replicas=0

# Airflow
helm repo add apache-airflow https://airflow.apache.org
helm install airflow apache-airflow/airflow -n orchestration \
  -f k8s/orchestration/airflow-values.yaml \
  --set images.airflow.tag=v7

# RBAC for Airflow
kubectl apply -f k8s/orchestration/airflow-rbac.yaml

# Create admin user
kubectl exec deployment/airflow-api-server -n orchestration -- \
  airflow users create --username admin --password admin \
  --firstname Admin --lastname User --role Admin --email admin@example.com

# Streamlit
kubectl apply -f k8s/serving/streamlit-deployment.yaml

# Monitoring
kubectl apply -f k8s/monitoring/
```

### 6. Access UIs

```bash
# Airflow (trigger DAG here)
kubectl port-forward svc/airflow-api-server -n orchestration 8080:8080

# Streamlit (view dashboard)
kubectl port-forward svc/streamlit -n serving 8501:8501

# Grafana (monitoring)
kubectl port-forward svc/grafana -n monitoring 3000:3000

# Prometheus
kubectl port-forward svc/prometheus -n monitoring 9090:9090
```

### 7. Run the pipeline

Open Airflow at http://localhost:8080, find `platform_pipeline`, and click the play button. The 7-task pipeline takes approximately 3-4 minutes. Each run accumulates new ride data.

## Troubleshooting

### LocalStack buckets missing after restart

LocalStack doesn't persist data across minikube restarts. Re-provision:

```bash
kubectl port-forward svc/localstack -n data 4566:4566 &
cd terraform && terraform apply -auto-approve
```

### Stuck PostgreSQL connections

If `load_staging` hangs or fails, check for stuck transactions:

```bash
kubectl exec deployment/postgres -n data -- psql -U platform -d warehouse \
  -c "SELECT pid, state, left(query, 60) FROM pg_stat_activity WHERE state != 'idle';"
```

Kill stuck connections:

```bash
kubectl exec deployment/postgres -n data -- psql -U platform -d warehouse \
  -c "SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid != pg_backend_pid() AND state != 'idle';"
```

### Airflow DAG not appearing

Verify the DAG file is in the image:

```bash
kubectl exec deployment/airflow-dag-processor -n orchestration -c dag-processor -- ls /opt/airflow/dags/
```

If empty, restart the dag-processor:

```bash
kubectl rollout restart deployment/airflow-dag-processor -n orchestration
```

### Spark job fails with "bucket does not exist"

Re-provision S3 buckets (see LocalStack section above). This happens after minikube restart.

### Streamlit connection error

Restart the Streamlit pod to reset connections:

```bash
kubectl rollout restart deployment/streamlit -n serving
```

## Shutdown

```bash
minikube stop       # Freezes cluster, preserves state
# Optional: wsl --shutdown   # Frees WSL2 memory
```

Resume with `minikube start` — all pods auto-restore.

## CI/CD

GitHub Actions validates on every push:
- Python linting (flake8) and formatting (black)
- Kubernetes manifest validation (kubeval)
- Terraform format and validate
- dbt project parsing
- Docker image builds for all 5 services

## Key Design Decisions

- **KubernetesExecutor over CeleryExecutor**: Each Airflow task runs in its own pod — no persistent workers consuming resources between runs
- **Strimzi over Bitnami Kafka**: Bitnami images hit paywall restrictions; Strimzi is the production standard for Kafka on K8s
- **KRaft mode (no Zookeeper)**: Kafka 4.0 native metadata management, simpler architecture
- **Custom Airflow image**: Bundles dbt, kubectl, boto3, psycopg — all task dependencies in one image
- **LocalStack + Terraform**: Same IaC deploys to real AWS by changing the endpoint URL
- **Accumulating rides**: Each pipeline run appends new data (ON CONFLICT DO NOTHING), growing the dataset over time
- **dbt for transformation**: SQL-based transformations with built-in testing, documentation, and lineage — not embedded in Python

## Portfolio Context

This is Project 9 of 13 in a Data Engineering roadmap. Previous projects built individual skills (Python ETL, SQL, Docker, Airflow, dbt, Kafka, Spark, Terraform, Kubernetes); this project integrates them into a production-style platform.
