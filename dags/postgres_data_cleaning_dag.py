from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import psycopg2
from io import StringIO
from airflow.models import Variable  # Correct import path

default_args = {
    'owner': 'hamza',
    'retries': 1,
    'retry_delay': timedelta(minutes=2)
}

def clean_data(df):
    try:
        df = df.drop_duplicates()
        df = df.replace('', pd.NA)

        # Normalize string columns
        for col in df.select_dtypes(include=['object']).columns:
            df[col] = df[col].astype(str).str.strip()

        # Normalize identifier columns (like customer_id, advisor_id, etc.)
        id_columns = [col for col in df.columns if 'id' in col.lower()]
        for col in id_columns:
            df[col] = df[col].astype(str).str.strip().str.upper()

        # Convert possible numeric columns
        for col in df.columns:
            try:
                if df[col].dtype == 'object':
                    sample = df[col].dropna().head(100)
                    if all(pd.to_numeric(sample, errors='coerce').notna()):
                        df[col] = pd.to_numeric(df[col], errors='coerce')
            except:
                pass

        # Handle date/time
        for col in df.columns:
            if df[col].dtype == 'object' and any(k in col.lower() for k in ['date', 'time', 'day', 'month', 'year']):
                try:
                    df[col] = pd.to_datetime(df[col], errors='coerce')
                except:
                    pass

        df = df.dropna(how='all')
        return df

    except Exception as e:
        print(f"Error cleaning data: {str(e)}")
        raise

def get_raw_tables():
    """Get list of tables from the raw database"""
    try:
        conn = psycopg2.connect(
            host=Variable.get("EC2_PUBLIC_IP"),
            dbname='finance_raw_db',
            user='airflow_user',
            password='airflow_pass',
            port='5432'
        )
        cursor = conn.cursor()

        try:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'airflow_data'")
            tables = cursor.fetchall()
            schema = "airflow_data"
        except:
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'airflow_user'")
            tables = cursor.fetchall()
            schema = "airflow_user"

        table_names = [table[0] for table in tables]
        return schema, table_names

    except Exception as e:
        print(f"Error getting table list: {str(e)}")
        raise
    finally:
        if 'cursor' in locals() and cursor:
            cursor.close()
        if 'conn' in locals() and conn:
            conn.close()

def process_table(table_name, source_schema):
    try:
        # Connect to raw database
        raw_conn = psycopg2.connect(
            host=Variable.get("EC2_PUBLIC_IP"),
            dbname='finance_raw_db',
            user='airflow_user',
            password='airflow_pass',
            port='5432'
        )
        query = f"SELECT * FROM {source_schema}.{table_name}"
        df = pd.read_sql_query(query, raw_conn)
        print(f"Read {len(df)} rows from {source_schema}.{table_name}")
        raw_conn.close()

        # Clean the data
        cleaned_df = clean_data(df)
        print(f"After cleaning: {len(cleaned_df)} rows")

        # Connect to clean database
        clean_conn = psycopg2.connect(
            host=Variable.get("EC2_PUBLIC_IP"),
            dbname='finance_clean_db',
            user='airflow_user',
            password='airflow_pass',
            port='5432'
        )
        clean_cursor = clean_conn.cursor()

        # Create schema if allowed
        try:
            clean_cursor.execute("CREATE SCHEMA IF NOT EXISTS clean_data;")
            clean_conn.commit()
            clean_schema = "clean_data"
        except psycopg2.errors.InsufficientPrivilege:
            clean_schema = "airflow_user"
            print(f"Using default schema '{clean_schema}' due to permission issues.")

        # Generate CREATE TABLE SQL
        columns = []
        for col in cleaned_df.columns:
            dtype = cleaned_df[col].dtype
            if pd.api.types.is_integer_dtype(dtype):
                col_type = "INTEGER"
            elif pd.api.types.is_float_dtype(dtype):
                col_type = "NUMERIC"
            elif pd.api.types.is_datetime64_dtype(dtype):
                col_type = "TIMESTAMP"
            else:
                col_type = "TEXT"
            columns.append(f"\"{col}\" {col_type}")

        create_table_query = f"CREATE TABLE IF NOT EXISTS {clean_schema}.{table_name} (" + ", ".join(columns) + ");"
        clean_cursor.execute(create_table_query)

        clean_cursor.execute(f"TRUNCATE TABLE {clean_schema}.{table_name};")

        # Load data using COPY
        buffer = StringIO()
        cleaned_df.to_csv(buffer, index=False, header=False, na_rep='NULL')
        buffer.seek(0)
        column_list = ", ".join([f'"{col}"' for col in cleaned_df.columns])
        copy_query = f"COPY {clean_schema}.{table_name} ({column_list}) FROM STDIN WITH CSV NULL 'NULL'"
        clean_cursor.copy_expert(copy_query, buffer)

        clean_conn.commit()
        print(f"Successfully loaded {len(cleaned_df)} rows into {clean_schema}.{table_name}")

    except Exception as e:
        print(f"Error processing table {table_name}: {str(e)}")
        raise
    finally:
        if 'clean_cursor' in locals() and clean_cursor:
            clean_cursor.close()
        if 'clean_conn' in locals() and clean_conn:
            clean_conn.close()

def process_all_tables():
    source_schema, tables = get_raw_tables()
    for table in tables:
        process_table(table, source_schema)

# Define the DAG
with DAG(
    dag_id='postgres_data_cleaning_pipeline',
    default_args=default_args,
    description='Clean data from finance_raw_db and load to finance_clean_db',
    schedule='@daily',  # Fixed key name
    start_date=datetime(2025, 5, 13),
    catchup=False
) as dag:

    clean_and_load = PythonOperator(
        task_id='clean_and_load_tables',
        python_callable=process_all_tables
    )

    clean_and_load

