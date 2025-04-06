from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os
import duckdb
import sys
sys.path.append("/opt/airflow")

from etl import dim_customer, dim_channel, dim_time, dim_location, dim_device, dim_type, fact_transaction


def extract_data(**kwargs):
    base_path = os.path.dirname(os.path.abspath(__file__))
    data_path = os.path.normpath(os.path.join(base_path, "..", "data", "transactions.csv"))
    df = pd.read_csv(data_path)
    kwargs['ti'].xcom_push(key="raw_data", value=df.to_json(orient="records"))
    print("✅ Data berhasil di-extract")


def transform_data(**kwargs):
    ti = kwargs['ti']
    raw_data = ti.xcom_pull(task_ids="extract_data", key="raw_data")
    df = pd.read_json(raw_data)
    df = df.drop_duplicates()
    kwargs['ti'].xcom_push(key="transformed_data", value=df.to_json(orient="records"))
    print("🔧 Transformasi umum selesai")


def load_data(name, etl_func):
    def task(**kwargs):
        ti = kwargs['ti']
        df = pd.read_json(ti.xcom_pull(task_ids="transform_data", key="transformed_data"))
        con = duckdb.connect("db/dwh-perbankan.duckdb")
        etl_func(df, con)
        con.close()
        print(f"✅ {name} berhasil diproses")
    return task


with DAG(
    dag_id="etl_pipeline_dag",
    start_date=datetime(2024, 1, 1),
    schedule_interval="@daily",
    catchup=False,
    tags=["etl", "dwh", "perbankan"],
) as dag:

    extract_task = PythonOperator(
        task_id="extract_data",
        python_callable=extract_data,
    )

    transform_task = PythonOperator(
        task_id="transform_data",
        python_callable=transform_data,
    )

    load_customer = PythonOperator(
        task_id="load_dim_customer",
        python_callable=load_data("Dim Customer", dim_customer.etl_dim_customer),
    )

    load_channel = PythonOperator(
        task_id="load_dim_channel",
        python_callable=load_data("Dim Channel", dim_channel.etl_dim_channel),
    )

    load_time = PythonOperator(
        task_id="load_dim_time",
        python_callable=load_data("Dim Time", dim_time.etl_dim_time),
    )

    load_location = PythonOperator(
        task_id="load_dim_location",
        python_callable=load_data("Dim Location", dim_location.etl_dim_location),
    )

    load_device = PythonOperator(
        task_id="load_dim_device",
        python_callable=load_data("Dim Device", dim_device.etl_dim_device),
    )

    load_type = PythonOperator(
        task_id="load_dim_type",
        python_callable=load_data("Dim Type", dim_type.etl_dim_type),
    )

    load_fact = PythonOperator(
        task_id="load_fact_transaction",
        python_callable=load_data("Fact Transaction", fact_transaction.etl_fact_transaction),
    )

    extract_task >> transform_task >> [
        load_customer,
        load_channel,
        load_time,
        load_location,
        load_device,
        load_type,
    ] >> load_fact
