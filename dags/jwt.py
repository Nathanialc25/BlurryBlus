from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from helpers.apple_auth import AppleAuthManager
import os

# Configuration - Use environment variables or Airflow Variables
JWT_PATH = "/opt/airflow/dags/apple_jwt.txt"
PRIVATE_KEY_PATH = "/opt/airflow/config/apple_private_key.p8"

# Get these from environment variables (set in docker-compose)
TEAM_ID = os.environ.get("APPLE_TEAM_ID") 
KEY_ID = os.environ.get("APPLE_KEY_ID")

def check_and_generate_jwt(**kwargs):
    """Main function to check and generate JWT if needed"""
    auth_manager = AppleAuthManager(
        team_id=TEAM_ID,
        key_id=KEY_ID,
        private_key_path=PRIVATE_KEY_PATH,
        jwt_store_path=JWT_PATH
    )
    
    token = auth_manager.ensure_valid_jwt()
    kwargs['ti'].xcom_push(key="apple_jwt", value=token)
    return token

default_args = {
    "owner": "airflow",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="apple_jwt_refresh",
    start_date=datetime(2025, 7, 30),
    schedule="@weekly",
    catchup=False,
    default_args=default_args,
    tags=["apple", "jwt"],
) as dag:

    generate_token = PythonOperator(
        task_id="generate_jwt_if_needed",
        python_callable=check_and_generate_jwt,
    )