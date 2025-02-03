import sys
import os
import logging
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import pandas as pd

# Get the absolute path of the project directory
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(BASE_DIR, ".."))  # Add parent directory to sys.path

# Import functions from your scripts
from scripts.extract import fetch_weather_data, save_data
from scripts.transform import clean_data, transform_data, save_transformed_data
from scripts.load import execute_sql_file, load_data_to_postgres

# Define the data path for where the files will be saved
DATA_PATH = os.path.join(BASE_DIR, "..", "data")

# Define the DAG
with DAG(
    'weather_etl',
    start_date=datetime(2023, 10, 1),
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Function to execute SQL table creation
    def execute_sql_task(**kwargs):
        execute_sql_file()

    # Extract task
    def extract_data_func(**kwargs):
        data_file = os.path.join(DATA_PATH, 'weather_data.csv')
        df = fetch_weather_data()  # Fetch data
        save_data(df, data_file)  # Save CSV
        logging.info(f'Extracted data saved to {data_file}')
        kwargs['ti'].xcom_push(key='data_file', value=data_file)

    extract_task = PythonOperator(
        task_id='extract',
        python_callable=extract_data_func
    )

    # Transform task
    def transform_data_func(**kwargs):
        ti = kwargs['ti']
        data_file = ti.xcom_pull(key='data_file', task_ids='extract')
        
        # Debugging: log the file path to ensure it's correct
        logging.info(f"Data file pulled from XCom: {data_file}")

        if not data_file:
            raise ValueError("Data file path is None. Check if 'save_data' is correctly saving the file.")
        
        transformed_file = os.path.join(DATA_PATH, 'transformed_weather_data.csv')
        df = pd.read_csv(data_file)
        df = clean_data(df)  # Clean data
        df = transform_data(df)  # Transform data
        save_transformed_data(df, transformed_file)  # Save transformed data
        logging.info(f'Transformed data saved to {transformed_file}')
        
        # Push transformed file path to XCom for use in the load task
        ti.xcom_push(key='transformed_file', value=transformed_file)

    transform_task = PythonOperator(
        task_id='transform',
        python_callable=transform_data_func
    )

    # Load task (execute SQL and then load data)
    def load_data_func(**kwargs):
        ti = kwargs['ti']
        
        # Get transformed file from XCom
        transformed_file = ti.xcom_pull(key='transformed_file', task_ids='transform')
        
        if not transformed_file:
            raise ValueError("Transformed file path is None. Ensure that the 'transform' task is correctly saving the file.")
        
        logging.info(f"Loading data from transformed file: {transformed_file}")
        
        # Call the load function to insert the data into PostgreSQL
        load_data_to_postgres(transformed_file)

    load_task = PythonOperator(
        task_id='load',
        python_callable=load_data_func
    )

    # Execute SQL task before loading data into PostgreSQL
    execute_sql_task = PythonOperator(
        task_id='execute_sql_task',
        python_callable=execute_sql_task
    )

    # Task dependencies
    execute_sql_task >> extract_task >> transform_task >> load_task








