from airflow import DAG
from airflow.operators.python import PythonOperator
import requests
import pandas as pd
from datetime import datetime
#import blockchain_data_etl # Importing our ETL script here
from sqlalchemy import create_engine, text
#import sys
#sys.path.append('/Users/muhammadchaudhary/airflow/dags/scripts')


def fetch_blockchain_data():
    #blockchain_data_etl.main()
    # API Endpoint from Coingecko to get market data
    url = 'https://api.coingecko.com/api/v3/coins/markets'
    params = {
        'vs_currency': 'usd',
        'ids': 'bitcoin,ethereum,cardano,binancecoin'
    }

    # Calling the API to fetch data
    try:
        response = requests.get(url, params=params)
        if response.status_code == 200:
            data = response.json()
            print('Blockchain data fetched successfully.')
        else:
            print('Failed to retrive data.')
    except Exception as e:
        print(f'Error occured while calling the endpoint to fetch data: {e}')

    # Convert data into a dataframe
    df = pd.DataFrame(data)
    print(df.head())
    return df

def transform_blockchain_data(ti):
    # pulling dataframe from XCom (communication of tasks in Airflow)
    df = ti.xcom_pull(task_ids='fetch_data')

    # transforming such as renaming columns
    df.rename(columsn={
        'id': 'coin_id',
        'symbol': 'coin_symbol',
        'name': 'coin_name',
        'current_price': 'price_usd',
        'market_cap': 'market_cap_usd',
        'total_supply': 'total_supply'
    })
    # Handle Missing Values here
    df.fillna(0, inplace=True)

    # any further filtering if required by business can be done here ....

    ti.xcom_push(key='tranformed_data', value=df)
    return df

def load_blockchain_data(ti):
    df = ti.xcom_pull(task_ids='transform_blockchain_data', key='tranformed_data')

    db_url = 'postgresql://dbt_user:Hammad12@localhost:5432/blockchain_etl_test'
    engine = create_engine(db_url)

    # SQL to create table if it doesn't exist
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS blockchain_data (
        coin_id VARCHAR PRIMARY KEY,
        coin_symbol VARCHAR,
        coin_name VARCHAR,
        price_usd FLOAT,
        market_cap_usd FLOAT,
        total_supply FLOAT
    );
    """

    with engine.connect() as connection:
        connection.execute(text(create_table_sql))

    df.to_sql("blockchain_data", engine, if_exists='replace', index=False)
    print("Data loaded to PostgreSQL successfully!")

# Airflow DAG
dag = DAG(
    'blockchain_etl_dag',
    description='ETL Pipeline for blockchain data',
    start_date=datetime(2025, 2, 10),
    schedule='@daily',
    catchup=False
)

# Define the Airflow tasks
extract_task = PythonOperator(
    task_id='fetch_blockchain_data',
    python_callable=fetch_blockchain_data,
    dag=dag)

transform_task = PythonOperator(
    task_id='transform_blockchain_data',
    python_callable=transform_blockchain_data,
    provide_context=True,  # Ensure the task gets context (XComs)
    dag=dag)

load_task = PythonOperator(
    task_id='load_blockchain_data',
    python_callable=load_blockchain_data,
    provide_context=True,  # Ensure the task gets context (XComs)
    dag=dag)

# Set task dependencies: Extract -> Transform -> Load
extract_task >> transform_task >> load_task