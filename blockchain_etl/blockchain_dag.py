from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import blockchain_data_etl # Importing our ETL script here


def fetch_data():
    blockchain_data_etl.main()

# Airflow DAG
dag = DAG(
    'blockchain_etl_dag',
    description='ETL Pipeline for blockchain data',
    start_date=datetime(2025, 2, 10),
    schedule='@daily',
    catchup=False
)

t1 = PythonOperator(
    task_id='fetch_blockchain_data',
    python_callable=fetch_data,
    dag=dag)

t1