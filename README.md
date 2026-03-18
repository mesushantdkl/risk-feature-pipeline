# Automated Risk Feature Pipeline for AI/ML Systems

![Python](https://img.shields.io/badge/Python-3.13-blue)
![dbt](https://img.shields.io/badge/dbt-Core-orange)
![Airflow](https://img.shields.io/badge/Airflow-2.8-green)
![PostgreSQL](https://img.shields.io/badge/PostgreSQL-15-blue)
![Docker](https://img.shields.io/badge/Docker-Compose-lightblue)
![Cost](https://img.shields.io/badge/Cost-%240-brightgreen)

An end-to-end ELT data pipeline that generates mock financial risk data,
transforms it using dbt, and produces a **RAG-ready feature table** for
AI/ML systems — fully orchestrated with Apache Airflow and running
locally in Docker at zero cost.

---

## Architecture
```
Python Generator → PostgreSQL (source) → ELT Sync → PostgreSQL (dest)
                                                           ↓
                                               dbt (staging → intermediate → mart)
                                                           ↓
                                               RAG-Ready Risk Feature Table
                                                           ↓
                                               Airflow DAG (daily schedule)
```

---

## Tech Stack

| Tool | Purpose |
|---|---|
| Python | Mock transaction data generation & ELT sync |
| PostgreSQL 15 | Source and destination databases |
| dbt Core | Data modelling, risk scoring, feature engineering |
| Apache Airflow 2.8 | Pipeline orchestration and scheduling |
| Docker Compose | Local infrastructure (zero cost) |

---

## Features

- **5,000 mock financial transactions** with realistic risk patterns
- **Multi-dimensional risk scoring** — velocity, geographic, and merchant risk
- **Jinja-powered dynamic filtering** — change risk threshold at runtime without editing SQL
- **RAG-ready feature table** — each row is a natural language transaction description ready for vector embedding
- **dbt data quality tests** — 5 automated tests for nulls, uniqueness, and score ranges
- **Airflow DAG** with dependency management and a Python validation gate
- **Full troubleshooting history** visible in Airflow UI grid

---

## Risk Score Distribution

| Risk Score | Count | % | Meaning |
|---|---|---|---|
| 1 — Low | 2,629 | 52.6% | Normal transactions |
| 2 — Medium | 2,273 | 45.5% | Minor risk indicators |
| 3 — High | 98 | 2.0% | Multiple risk factors, flagged |

---

## Sample RAG Feature Output

Each row in the final `risk_feature_table` looks like this:
```
Transaction 99748992-b026-4d9b-bd40-d357fb1ab994 from account ACC4519
processed a transfer of $21329.23 in unknown (NG). Risk score: 3/5.
Velocity score: 3. Geographic risk: 3. Merchant risk: 2.
FLAGGED for review. Account avg transaction: $11120.27.
Total flagged this account: 2.
```

This text can be passed directly to an embedding model and stored in a
vector database for semantic retrieval by a RAG system.

---

## Project Structure
```
risk-pipeline/
├── docker-compose.yml
├── data_generator/
│   ├── generate_transactions.py   # Generates 5,000 mock transactions
│   └── airbyte_sync.py            # ELT sync: source → destination
├── dbt_project/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/               # stg_transactions (view)
│   │   ├── intermediate/          # int_risk_scores (view, CTEs + Jinja)
│   │   └── marts/                 # risk_feature_table (table, RAG-ready)
│   └── tests/
│       └── schema.yml             # 5 dbt data quality tests
└── airflow/
    └── dags/
        └── risk_pipeline_dag.py   # Airflow DAG (3 tasks, daily schedule)
```

---

## How to Run

**Prerequisites:** Docker Desktop, Python 3.8+
```bash
# 1. Start all containers
docker compose up -d

# 2. Seed source database
python3 data_generator/generate_transactions.py

# 3. Sync to destination (ELT)
python3 data_generator/airbyte_sync.py

# 4. Install dbt and run transformations
pip install dbt-postgres
cd dbt_project
dbt run --vars '{"risk_level": "high"}' --profiles-dir .
dbt test --profiles-dir .

# 5. Open Airflow UI → http://localhost:8080 (admin / admin)
docker exec -it risk_airflow airflow users create \
  --username admin --password admin \
  --firstname Risk --lastname Team \
  --role Admin --email risk@local.com

# 6. Trigger the pipeline
docker exec -it risk_airflow airflow dags trigger risk_feature_pipeline
```

---

## Airflow Pipeline

The DAG runs three tasks in sequence:
```
dbt_transform → dbt_test → validate_for_ai_ml
```

If any task fails, all downstream tasks are skipped automatically —
ensuring the AI/ML team never receives incomplete data.

---

## dbt Model Layers

### Staging
Cleans and standardises raw data — lowercases categories, filters nulls,
truncates timestamps.

### Intermediate
Calculates risk scores using CTEs and a Jinja variable:
```sql
{% set risk_level = var('risk_level', 'high') %}
{% set amount_threshold = 10000 if risk_level == 'high' else 5000 %}
```

### Mart
Produces the RAG-ready `risk_feature_table` with both structured fields
and a `feature_text` natural language column per transaction.

---

## Data Quality

| Test | Model | Check |
|---|---|---|
| not_null | stg_transactions | transaction_id never null |
| unique | stg_transactions | transaction_id no duplicates |
| not_null | stg_transactions | amount never null |
| not_null | int_risk_scores | risk_score never null |
| not_null | risk_feature_table | feature_text never null |

---

*Built as a portfolio project demonstrating ELT pipeline design,
dbt modelling, Airflow orchestration, and AI/ML data engineering.*
