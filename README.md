# Weather ETL Pipeline with Apache Airflow

This project is a complete **ETL (Extract, Transform, Load) pipeline** that fetches weather data from the **OpenWeather API**, processes it, and loads it into a **PostgreSQL database**. The pipeline is orchestrated using **Apache Airflow**.

The dataset consists of weather data from **100 cities in China** as an example.

🚀 **Key Technologies:** Python, Apache Airflow, PostgreSQL, Pandas  

## Project Structure
```
.
├── dags
│   └── weather_etl.py        # Airflow DAG definition
├── data                      # Directory for storing extracted and transformed data
├── scripts
│   ├── extract.py            # Extracts data from OpenWeather API
│   ├── load.py               # Loads data into PostgreSQL
│   └── transform.py          # Cleans and transforms the data
├── sql/
│   ├── create_table.sql      # SQL script to create database table
├── .env                      # Environment variables (not included in repo)
├── requirements.txt          # Python dependencies
└── README.md                 # Project documentation
```

## Setup and Installation

### 1. Clone the Repository
```bash
git clone https://github.com/brankowss/Weather-ETL-Apache-Airflow.git
cd Weather-ETL-Apache-Airflow
```

### 2. Create a Virtual Environment
Ensure you have Python installed, then create and activate a virtual environment:
```bash
python -m venv venv
source venv/bin/activate   # On macOS/Linux
venv\Scripts\activate      # On Windows
```

### 3. Install Dependencies
Install the required Python packages:
```bash
pip install -r requirements.txt
```

### 4. Get OpenWeather API Key
You need to sign up and generate an API key from **OpenWeather** to fetch weather data. Register and get your API key here:  
🔗 [OpenWeather API Key](https://home.openweathermap.org/users/sign_up)

### 5. Set Up Environment Variables
Create a `.env` file in the project root and configure your credentials:
```env
OPENWEATHERMAP_API_KEY=your_api_key_here
POSTGRES_HOST=localhost
POSTGRES_DB=database_name
POSTGRES_USER=your_username
POSTGRES_PASSWORD=your_password
POSTGRES_PORT=5432
```

### 6. Install Airflow and providers
```bash
pip install apache-airflow apache-airflow-providers-postgres
```
```bash
export AIRFLOW_HOME=$(pwd)/airflow  # On macOS/Linux
set AIRFLOW_HOME=%cd%\airflow      # On Windows
```
Configure Airflow (Important):
Add or modify the following settings in airflow.cfg
```bash
[core]
sql_alchemy_conn = postgresql+psycopg2://your_user:your_password@localhost/your_db_name  # If you want to use a PostgreSQL for metadata
executor = LocalExecutor  # For this project, LocalExecutor is sufficient
dags_folder =  $AIRFLOW_HOME/dags # Or a path if you are storing DAGs somewhere else
```
```bash
airflow db init
```
### 7. Create an Airflow User
To access the Airflow UI, create an admin user:
```bash
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

### 8. Start Airflow Scheduler & Webserver
In separate terminal windows, run:
```bash
airflow scheduler
```
```bash
airflow webserver --port 8080
```
Then, access the Airflow UI at [http://localhost:8080](http://localhost:8080).

### 9. Run the DAG
Once Airflow is running, trigger the ETL pipeline:
```bash
airflow dags trigger weather_etl
```
Alternatively, go to the **Airflow UI**, find `weather_etl`, and click **"Trigger DAG"**.

---

## How It Works
- **Extract:** `extract.py` fetches weather data from OpenWeather API and saves it as a CSV file.
- **Transform:** `transform.py` cleans the data.
- **Load:** `load.py` inserts into a PostgreSQL database.
- **Orchestration:** `weather_etl.py` defines the workflow in Airflow.
