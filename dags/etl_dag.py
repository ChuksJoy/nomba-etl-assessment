from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd
import psycopg2
from pymongo import MongoClient

# ----- EXTRACT FUNCTIONS -----
def extract_user(**context):
    mongo_client = MongoClient("mongodb://nomba_user:nomba_pass@mongo_source:27017/")
    db = mongo_client["nomba_source"]
    users = list(db["user"].find({}, {"_id": 0}))  # exclude _id
    mongo_client.close()
    context['ti'].xcom_push(key='user_data', value=pd.DataFrame(users).to_json())

def extract_savings_plan(**context):
    conn = psycopg2.connect(
        dbname="postgres_source",
        user="user",
        password="password",
        host="postgres_source",
        port="5432"
    )
    df = pd.read_sql("SELECT * FROM savings_plan;", conn)
    conn.close()
    context['ti'].xcom_push(key='plan_data', value=df.to_json())

def extract_savings_transaction(**context):
    conn = psycopg2.connect(
        dbname="postgres_source",
        user="user",
        password="password",
        host="postgres_source",
        port="5432"
    )
    df = pd.read_sql("SELECT * FROM savings_transaction;", conn)
    conn.close()
    context['ti'].xcom_push(key='transaction_data', value=df.to_json())

# ----- LOAD FUNCTIONS -----
def load_user(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='user_data', task_ids='extract_user'))
    conn = psycopg2.connect(
        dbname="analytics_warehouse",
        user="warehouse_user",
        password="warehouse_password",
        host="data_warehouse",
        port="5433"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_user (
            uid TEXT PRIMARY KEY,
            first_name TEXT,
            last_name TEXT,
            occupation TEXT,
            state TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO analytics_user (uid, first_name, last_name, occupation, state, created_at, updated_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT(uid) DO UPDATE
            SET first_name = EXCLUDED.first_name,
                last_name = EXCLUDED.last_name,
                occupation = EXCLUDED.occupation,
                state = EXCLUDED.state,
                updated_at = EXCLUDED.updated_at;
        """, (row['Uid'], row['firstName'], row['lastName'], row['occupation'], row['state'], row['created_at'], row['updated_at']))
    conn.commit()
    conn.close()

def load_savings_plan(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='plan_data', task_ids='extract_savings_plan'))
    conn = psycopg2.connect(
        dbname="analytics_warehouse",
        user="warehouse_user",
        password="warehouse_password",
        host="data_warehouse",
        port="5433"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_savings_plan (
            plan_id TEXT PRIMARY KEY,
            product_type TEXT,
            customer_uid TEXT,
            amount NUMERIC,
            frequency TEXT,
            start_date DATE,
            end_date DATE,
            status TEXT,
            created_at TIMESTAMP,
            updated_at TIMESTAMP
        );
    """)
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO analytics_savings_plan (plan_id, product_type, customer_uid, amount, frequency, start_date, end_date, status, created_at, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(plan_id) DO UPDATE
            SET product_type = EXCLUDED.product_type,
                customer_uid = EXCLUDED.customer_uid,
                amount = EXCLUDED.amount,
                frequency = EXCLUDED.frequency,
                start_date = EXCLUDED.start_date,
                end_date = EXCLUDED.end_date,
                status = EXCLUDED.status,
                updated_at = EXCLUDED.updated_at;
        """, (row['plan_id'], row['product_type'], row['customer_uid'], row['amount'], row['frequency'], row['start_date'], row['end_date'], row['status'], row['created_at'], row['updated_at']))
    conn.commit()
    conn.close()

def load_savings_transaction(**context):
    df = pd.read_json(context['ti'].xcom_pull(key='transaction_data', task_ids='extract_savings_transaction'))
    conn = psycopg2.connect(
        dbname="analytics_warehouse",
        user="warehouse_user",
        password="warehouse_password",
        host="data_warehouse",
        port="5433"
    )
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS analytics_savings_transaction (
            txn_id TEXT PRIMARY KEY,
            plan_id TEXT,
            amount NUMERIC,
            currency TEXT,
            side TEXT,
            rate NUMERIC,
            txn_timestamp TIMESTAMP,
            updated_at TIMESTAMP,
            deleted_at TIMESTAMP
        );
    """)
    for _, row in df.iterrows():
        cursor.execute("""
            INSERT INTO analytics_savings_transaction (txn_id, plan_id, amount, currency, side, rate, txn_timestamp, updated_at, deleted_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT(txn_id) DO UPDATE
            SET plan_id = EXCLUDED.plan_id,
                amount = EXCLUDED.amount,
                currency = EXCLUDED.currency,
                side = EXCLUDED.side,
                rate = EXCLUDED.rate,
                txn_timestamp = EXCLUDED.txn_timestamp,
                updated_at = EXCLUDED.updated_at,
                deleted_at = EXCLUDED.deleted_at;
        """, (row['txn_id'], row['plan_id'], row['amount'], row['currency'], row['side'], row['rate'], row['txn_timestamp'], row['updated_at'], row['deleted_at']))
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
    dag_id='etl_pipeline_nomba',
    default_args=default_args,
    description='ETL Nomba: Mongo + Postgres -> Warehouse',
    start_date=datetime(2025, 11, 9),
    schedule_interval='@daily',
    catchup=False,
    tags=['etl', 'nomba'],
) as dag:

    # Extract tasks
    extract_user_task = PythonOperator(task_id='extract_user', python_callable=extract_user)
    extract_plan_task = PythonOperator(task_id='extract_savings_plan', python_callable=extract_savings_plan)
    extract_txn_task = PythonOperator(task_id='extract_savings_transaction', python_callable=extract_savings_transaction)

    # Load tasks
    load_user_task = PythonOperator(task_id='load_user', python_callable=load_user)
    load_plan_task = PythonOperator(task_id='load_savings_plan', python_callable=load_savings_plan)
    load_txn_task = PythonOperator(task_id='load_savings_transaction', python_callable=load_savings_transaction)

    # Set dependencies correctly using zip
    for extract, load in zip(
        [extract_user_task, extract_plan_task, extract_txn_task],
        [load_user_task, load_plan_task, load_txn_task]
    ):
        extract >> load
