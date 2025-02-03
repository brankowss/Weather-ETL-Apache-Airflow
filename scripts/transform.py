import pandas as pd
import logging
from datetime import datetime

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def clean_data(df):
    """
    Cleans the dataset by:
    - Checking for missing values and removing rows with nulls.
    - Converting Unix timestamp into a human-readable datetime format.
    """

    # Check for missing values
    if df.isnull().values.any():
        logging.warning("Missing values found. Dropping rows with missing data.")
        df.dropna(inplace=True)

    # Convert Unix timestamp to readable datetime format
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    
    # Remove the original timestamp column
    df.drop(columns=['timestamp'], inplace=True)

    return df

def transform_data(df):
    """
    Transforms the dataset by:
    - Adding a column for temperature in Fahrenheit.
    - Formatting the weather description in title case.
    """

    # Convert Celsius to Fahrenheit
    df['temperature_f'] = (df['temperature'] * 9/5) + 32

    # Format weather description to title case
    df['weather'] = df['weather'].str.title()

    return df

def save_transformed_data(df, transformed_file):
    """
    Saves the transformed dataset to a CSV file.
    """
    df.to_csv(transformed_file, index=False)
    logging.info(f"Transformed data saved to {transformed_file}")

if __name__ == "__main__":
    # Load raw data
    input_file = "data/weather_data.csv"
    logging.info(f"Loading data from {input_file}")
    df = pd.read_csv(input_file)

    # Clean the data
    df = clean_data(df)

    # Transform the data
    df = transform_data(df)

    # Save the transformed data
    save_transformed_data(df)
