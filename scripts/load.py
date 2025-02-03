import pandas as pd
import psycopg2
import logging
import os
from dotenv import load_dotenv

# Load environment variables (for database credentials)
dotenv_path = os.path.join(os.path.dirname(__file__), '..', '.env')
load_dotenv(dotenv_path)

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# Get database credentials from environment variables
DB_NAME = os.getenv("POSTGRES_DB")
DB_USER = os.getenv("POSTGRES_USER")
DB_PASSWORD = os.getenv("POSTGRES_PASSWORD")
DB_HOST = os.getenv("POSTGRES_HOST", "localhost")
DB_PORT = os.getenv("POSTGRES_PORT", "5432")

def execute_sql_file():
    """Executes SQL commands from sql/create_table.sql to ensure the table exists."""
    sql_file_path = os.path.join(os.path.dirname(__file__), '..', 'sql', 'create_table.sql')

    try:
        # Connect to PostgreSQL database
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()

        # Open and execute SQL file
        with open(sql_file_path, 'r') as sql_file:
            cur.execute(sql_file.read())

        # Commit changes and close connection
        conn.commit()
        cur.close()
        conn.close()
        logging.info("Table checked/created successfully.")

    except Exception as e:
        logging.error(f"Error executing SQL file: {e}")
        raise

def load_data_to_postgres(transformed_file):
    """Loads the transformed data into PostgreSQL"""
    try:
        conn = psycopg2.connect(
            dbname=DB_NAME, user=DB_USER, password=DB_PASSWORD, host=DB_HOST, port=DB_PORT
        )
        cur = conn.cursor()

        # Open the transformed CSV file
        with open(transformed_file, 'r') as f:
            next(f)  # Skip the header row
            cur.copy_from(f, 'weather_data', sep=',', columns=('city', 'temperature', 'humidity', 'weather', 'pressure', 'wind_speed', 'visibility', 'datetime', 'temperature_f'))

        conn.commit()
        cur.close()
        conn.close()
        logging.info(f"Data from {transformed_file} loaded into PostgreSQL")

    except Exception as e:
        logging.error(f"Error loading data into PostgreSQL: {e}")
        raise
    
if __name__ == "__main__":
    execute_sql_file()  # Ensure table exists
    csv_path = os.path.join(os.path.dirname(__file__), "..", "data", "transformed_weather_data.csv")
    load_data_to_postgres(csv_path)  # Load data

