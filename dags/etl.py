from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import Connection
from airflow.utils.db import provide_session
from airflow.hooks.base import BaseHook
import pandas as pd
import logging
import os
from airflow.providers.postgres.operators.postgres import PostgresOperator

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': True,
    'email_on_retry': False,
    'email': 'airflow@example.com',
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'start_date': datetime(2024, 9, 1),  # Start date for the DAG
    'catchup': False,  # Disable backfilling
}

dag = DAG(
    'amazon_etl',
    default_args=default_args,
    description='ETL pipeline for amazon sales data',
    schedule_interval=timedelta(days=1),  # Run daily
    catchup=False,
)

@provide_session
def create_postgres_connection(session = None):
    conn_id = 'postgres_docker'
    try:
        conn = session.query(Connection).filter(Connection.conn_id == conn_id).first()
        if conn is None:
            logging.info(f"Creating new PostgreSQL connection with conn_id: {conn_id}")
            new_conn = Connection(
                conn_id=conn_id,
                conn_type='postgres',
                host='postgres-data',
                schema='amazon',
                login='postgres',
                password='postgres',
                port=5432
            )
            session.add(new_conn)
            session.commit()
            logging.info(f"Connection {conn_id} created successfully!")
        else:
            logging.info(f"Connection {conn_id} already exists.")
    except Exception as e:
        logging.error(f"Error creating connection: {e}")
        raise

def extract_data(**kwargs):
    """Extract data from CSV files and MySQL database."""
    # Extract from CSV
    csv_file_path = '/opt/airflow/data/amazon.csv'
    df = pd.read_csv(csv_file_path)
    
    # Push data to XCom for further tasks
    kwargs['ti'].xcom_push(key='amazon_data', value=df.to_dict())

    logging.info("Data extracted successfully.")

def transform_data(**kwargs):
    """Transform data: clean, enrich, and aggregate."""
    # Pull data from XCom
    amazon_data = pd.DataFrame(kwargs['ti'].xcom_pull(key='amazon_data'))

    # Data Cleaning
    amazon_data.dropna(inplace=True)
    amazon_data['created_date'] = pd.to_datetime(datetime.now())

    # Convert any Timestamp or datetime columns to string format
    amazon_data['created_date'] = amazon_data['created_date'].astype(str)

    amazon_data_selected = amazon_data[['product_id', 'product_name', 'category', 'discounted_price', 'actual_price', \
                                        'discount_percentage', 'rating', 'rating_count', 'created_date']]

    # Push transformed data to XCom
    kwargs['ti'].xcom_push(key='amazon_data', value=amazon_data_selected.to_dict())

    logging.info("Data transformed successfully.")


def load_data(**kwargs):
    """Load transformed data into PostgreSQL."""
    # Pull transformed data from XCom
    amazon_data = pd.DataFrame(kwargs['ti'].xcom_pull(key='amazon_data'))

    # Load into PostgreSQL
    postgres_hook = PostgresHook(postgres_conn_id='postgres_docker')
    postgres_hook.insert_rows(
        table='amazon.amazon_data',
        rows=amazon_data.values.tolist(),
        target_fields=['product_id', 'product_name', 'category', 'discounted_price', 'actual_price', \
                                        'discount_percentage', 'rating', 'rating_count', 'created_date'],
        commit_every=1000
    )

    logging.info("Data loaded successfully into PostgreSQL.")

def validate_data():
    """Validate loaded data in PostgreSQL."""
    postgres_hook = PostgresHook(postgres_conn_id='postgres_docker')
    records = postgres_hook.get_records(sql='SELECT COUNT(*) FROM amazon.amazon_data WHERE actual_price IS NULL')

    if records[0][0] > 0:
        raise ValueError("Data validation failed: Found NULL values in actual_price.")
    else:
        logging.info("Data validation passed.")

# Task: Create PostgreSQL connection
create_connection_task = PythonOperator(
    task_id='create_postgres_connection',
    python_callable=create_postgres_connection,
    dag=dag,
)

# Task: Extract data
extract_task = PythonOperator(
    task_id='extract_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

# Task: Transform data
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

# Task: Load data
load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Task: Validate data
validate_task = PythonOperator(
    task_id='validate_data',
    python_callable=validate_data,
    dag=dag,
)

create_table_task = PostgresOperator(
    task_id="create_table",
    postgres_conn_id="postgres_docker",
    sql="""
        CREATE SCHEMA IF NOT EXISTS amazon;
        DROP TABLE IF EXISTS amazon.amazon_data;
        CREATE TABLE amazon.amazon_data (
            product_id VARCHAR(20),    
            product_name TEXT, 
            category TEXT,     
            discounted_price VARCHAR(10),  
            actual_price VARCHAR(10),      
            discount_percentage VARCHAR(10),
            rating VARCHAR(10),       
            rating_count VARCHAR(10), 
            created_date TIMESTAMP 
    );
    """,
    dag=dag,
)

create_connection_task >> extract_task >> transform_task >> create_table_task >> load_task >> validate_task
