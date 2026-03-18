from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta
import psycopg2

default_args = {
    "owner": "risk-team",
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
    "email_on_failure": False,
}

def validate_data_quality():
    conn = psycopg2.connect(
        host="postgres_dest",
        port=5432,
        dbname="risk_dest",
        user="admin",
        password="password"
    )
    cur = conn.cursor()

    checks = {
        "no_null_transaction_ids": """
            SELECT COUNT(*) FROM risk_feature_table
            WHERE transaction_id IS NULL
        """,
        "risk_score_in_range": """
            SELECT COUNT(*) FROM risk_feature_table
            WHERE risk_score NOT BETWEEN 1 AND 5
        """,
        "feature_text_populated": """
            SELECT COUNT(*) FROM risk_feature_table
            WHERE feature_text IS NULL OR LENGTH(feature_text) < 10
        """,
    }

    failures = []
    for check_name, sql in checks.items():
        cur.execute(sql)
        count = cur.fetchone()[0]
        if count > 0:
            failures.append(f"{check_name}: {count} violations")
        else:
            print(f"PASS — {check_name}")

    cur.close()
    conn.close()

    if failures:
        raise ValueError(f"Data quality failures: {failures}")
    print("All quality checks passed. Data is safe for AI/ML.")

with DAG(
    dag_id="risk_feature_pipeline",
    default_args=default_args,
    description="ELT pipeline for AI/ML risk features",
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=["risk", "ml", "elt"],
) as dag:

    task_dbt_run = BashOperator(
        task_id="dbt_transform",
        bash_command="/home/airflow/.local/bin/dbt run --vars '{risk_level: high}' --profiles-dir /opt/dbt --project-dir /opt/dbt",
    )

    task_dbt_test = BashOperator(
        task_id="dbt_test",
        bash_command="/home/airflow/.local/bin/dbt test --profiles-dir /opt/dbt --project-dir /opt/dbt",
    )

    task_validate = PythonOperator(
        task_id="validate_for_ai_ml",
        python_callable=validate_data_quality,
    )

    task_dbt_run >> task_dbt_test >> task_validate
