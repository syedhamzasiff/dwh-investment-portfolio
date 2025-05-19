from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator

# ────────────────
# Metabase settings
# ────────────────
METABASE_URL       = "http://localhost:3000"     # your meta2 container
METABASE_USER      = "syedhamzasiff@gmail.com"
METABASE_PASSWORD  = "metabase1"
METABASE_DB_ID     = 2                           # set to your database ID
DASHBOARD_IDS      = [2]                      # adjust to dashboards you want to refresh

# Default args for retries, etc.
default_args = {
    'owner': 'hamza',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id='metabase_dashboard_refresh',
    default_args=default_args,
    description='Refresh Metabase dashboard cards via API',
    schedule='@hourly',   # change as needed
    start_date=datetime(2025, 5, 19),
    catchup=False,
    tags=['metabase', 'dashboard'],
) as dag:

    def login_metabase(**context):
        """Authenticate to Metabase and push session token to XCom."""
        url = f"{METABASE_URL.rstrip('/')}/api/session"
        resp = requests.post(url, json={
            "username": METABASE_USER,
            "password": METABASE_PASSWORD,
        })
        resp.raise_for_status()
        return resp.json()['id']

    def sync_schema(**context):
        """Trigger a schema sync in Metabase."""
        token = context['ti'].xcom_pull(task_ids='login')
        headers = {'X-Metabase-Session': token}
        url = f"{METABASE_URL.rstrip('/')}/api/database/{METABASE_DB_ID}/sync_schema"
        resp = requests.post(url, headers=headers)
        resp.raise_for_status()
        return resp.status_code

    def refresh_dashboards(**context):
        """Fetch each dashboard's cards and re-run each card query."""
        token = context['ti'].xcom_pull(task_ids='login')
        headers = {'X-Metabase-Session': token}
        base = METABASE_URL.rstrip('/')

        for dash_id in DASHBOARD_IDS:
            dash = requests.get(f"{base}/api/dashboard/{dash_id}", headers=headers)
            dash.raise_for_status()
            cards = dash.json().get('ordered_cards', [])

            for entry in cards:
                card_id = entry['card_id']
                q_url = f"{base}/api/card/{card_id}/query/json"
                r = requests.post(q_url, headers=headers)
                r.raise_for_status()

        return f"Refreshed dashboards: {DASHBOARD_IDS}"

    login = PythonOperator(
        task_id='login',
        python_callable=login_metabase
    )

    sync = PythonOperator(
        task_id='sync',
        python_callable=sync_schema
    )

    refresh = PythonOperator(
        task_id='refresh',
        python_callable=refresh_dashboards
    )
    

    login >> sync >> refresh
