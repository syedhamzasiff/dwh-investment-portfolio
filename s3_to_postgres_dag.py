from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import boto3
import pandas as pd
import psycopg2
from io import StringIO
import os
from airflow.sdk import Variable  # Updated import to use the new SDK module

default_args = {
    'owner': 'hamza',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def download_files_from_s3():
    # Get AWS credentials from Airflow Variables
    aws_access_key = Variable.get("aws_access_key_id")
    aws_secret_key = Variable.get("aws_secret_access_key")
    
    # Create boto3 client with explicit credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    bucket_name = 'raw-financial-bucket-dwh'
    prefix = 'raw_data/'
    # List all CSV files in the S3 'raw_data' folder
    response = s3.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
    files = [obj['Key'] for obj in response.get('Contents', []) if obj['Key'].endswith('.csv')]
    return files

def load_file_to_postgres(file_name):
    # Get AWS credentials from Airflow Variables
    aws_access_key = Variable.get("aws_access_key_id")
    aws_secret_key = Variable.get("aws_secret_access_key")
    
    # Create boto3 client with explicit credentials
    s3 = boto3.client(
        's3',
        aws_access_key_id=aws_access_key,
        aws_secret_access_key=aws_secret_key
    )
    
    try:
        bucket_name = 'raw-financial-bucket-dwh'
        # Download the file from S3
        obj = s3.get_object(Bucket=bucket_name, Key=file_name)
        df = pd.read_csv(obj['Body'])
        
        # Create a PostgreSQL connection
        conn = psycopg2.connect(
            host= Variable.get("EC2_PUBLIC_IP"),
            dbname='finance_raw_db',
            user='airflow_user',
            password='airflow_pass',
            port='5432'
        )
        cursor = conn.cursor()
        
        # Define the table name from the file name (remove path and extension)
        table_name = os.path.splitext(os.path.basename(file_name))[0]
        
        # Check if user has schema creation privileges
        try:
            # Try to create a schema for our data if it doesn't exist
            cursor.execute("CREATE SCHEMA IF NOT EXISTS airflow_data;")
            conn.commit()
            schema = "airflow_data"
        except psycopg2.errors.InsufficientPrivilege:
            # If we can't create a schema, use the user's default schema (usually the username)
            schema = "airflow_user"
            print(f"Using default schema '{schema}' due to permission restrictions")
        
        # Create the table schema dynamically based on the CSV columns with explicit schema
        columns = df.columns
        create_table_query = f"CREATE TABLE IF NOT EXISTS {schema}.{table_name} (" + ", ".join([f"{col} TEXT" for col in columns]) + ");"
        cursor.execute(create_table_query)
        
        # Check if table already has data and truncate if needed
        cursor.execute(f"TRUNCATE TABLE {schema}.{table_name};")
        
        # Insert data into the table
        buffer = StringIO()
        df.to_csv(buffer, index=False, header=False)
        buffer.seek(0)
        
        # Use explicit schema in copy_from
        copy_query = f"COPY {schema}.{table_name} FROM STDIN WITH CSV"
        cursor.copy_expert(copy_query, buffer)
        
        conn.commit()
        print(f"Successfully loaded {len(df)} rows into {schema}.{table_name}")
        
    except Exception as e:
        print(f"Error processing file {file_name}: {str(e)}")
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def process_files():
    files = download_files_from_s3()
    for file in files:
        load_file_to_postgres(file)

# Define the DAG
with DAG(
    dag_id='s3_to_postgres_pipeline',
    default_args=default_args,
    description='Load all CSV data from S3 to Postgres as separate tables',
    schedule='@daily',
    start_date=datetime(2025, 5, 13),
    catchup=False
) as dag:
    # PythonOperator task to run the `process_files` function
    extract_and_load = PythonOperator(
        task_id='download_and_load_csv_files',
        python_callable=process_files
    )
    
    extract_and_load
