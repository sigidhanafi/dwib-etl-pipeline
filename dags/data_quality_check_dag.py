from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import duckdb

DB_PATH = "/opt/airflow/db/dwh-perbankan.duckdb"

def check_nulls():
    with duckdb.connect(DB_PATH) as con:
        null_count = con.execute("SELECT COUNT(*) FROM Fact_Transaction WHERE TransactionID IS NULL").fetchone()[0]
        if null_count > 0:
            raise ValueError(f"Terdapat {null_count} NULL di kolom TransactionID")

def check_customer_age_range():
    with duckdb.connect(DB_PATH) as con:
        invalid_count = con.execute("""
            SELECT COUNT(*) FROM Dim_Customer 
            WHERE CustomerAge < 17 OR CustomerAge > 80
        """).fetchone()[0]
        if invalid_count > 0:
            raise ValueError(f"Terdapat {invalid_count} baris dengan CustomerAge diluar batas wajar")

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 1),
    'retries': 1
}

with DAG(
    dag_id="data_quality_check_dag",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False,
    tags=["quality", "validation"]
) as dag:

    task_check_nulls = PythonOperator(
        task_id="check_null_transaction_id_nulls",
        python_callable=check_nulls
    )

    task_check_customer_age_range = PythonOperator(
        task_id="check_customer_age_range",
        python_callable=check_customer_age_range
    )

    task_check_nulls >> task_check_customer_age_range
