from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from pymongo import MongoClient
import os

# ----- EXTRACT FUNCTIONS -----
def extract_postgres(**context):
    conn = psycopg2.connect(
        dbname="source_db",
        user="user",
        password="password",
        host="postgres_source",
        port="5432"
    )
    df = pd.read_sql("SELECT * FROM customers;", conn)
    conn.close()
    context['ti'].xcom_push(key='pg_data', value=df.to_json())

def extract_mongo(**context):
    mongo_client = MongoClient("mongodb://user:password@mongo_source:27017/")
    db = mongo_client["test"]
    data = list(db["transactions"].find({}, {"_id": 0}))
    mongo_client.close()
    context['ti'].xcom_push(key='mongo_data', value=pd.DataFrame(data).to_json())

# ----- TRANSFORM FUNCTION -----
def transform(**context):
    import json
    pg_df = pd.read_json(context['ti'].xcom_pull(key='pg_data', task_ids='extract_postgres'))
    mongo_df = pd.read_json(context['ti'].xcom_pull(key='mongo_data', task_ids='extract_mongo'))
    
    merged = pd.merge(pg_df, mongo_df, on="customer_id", how="left")
    context['ti'].xcom_push(key='transformed_data', value=merged.to_json())

# ----- LOAD FUNCTION -----
def load_to_warehouse(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='transformed_data', task_ids='transform'))
    conn = psycopg2.connect(
        dbname="analytics_warehouse",
        user="warehouse_user",
        password="warehouse_password",
        host="data_warehouse",
        port="5432"
    )
    cursor = conn.cursor()

    cursor.execute("""
        CREATE TABLE IF NOT EXISTS customer_transactions (
            customer_id INT,
            name TEXT,
            transaction_amount FLOAT,
            transaction_date DATE
        );
    """)

    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO customer_transactions (customer_id, name, transaction_amount, transaction_date)
            VALUES (%s, %s, %s, %s);
        """, (row['customer_id'], row['name'], row['amount'], row['transaction_date']))

    conn.commit()
    conn.close()

# ----- DAG DEFINITION -----
default_args = {
    'owner': 'Joy',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG(
    dag_id='etl_pipeline',
    default_args=default_args,
    description='Extract from Postgres + Mongo, transform and load to warehouse',
    start_date=datetime(2025, 11, 1),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'nomba'],
) as dag:

    t1 = PythonOperator(
        task_id='extract_postgres',
        python_callable=extract_postgres
    )

    t2 = PythonOperator(
        task_id='extract_mongo',
        python_callable=extract_mongo
    )

    t3 = PythonOperator(
        task_id='transform',
        python_callable=transform
    )

    t4 = PythonOperator(
        task_id='load_to_warehouse',
        python_callable=load_to_warehouse
    )

    [t1, t2] >> t3 >> t4
