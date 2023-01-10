from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.postgres_operator import PostgresOperator


default_args = {
    'owner': 'udacity',
    'start_date': datetime(2022, 12, 24),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'catchup': False,
    'email_on_retry': False,
}

dag = DAG(
    "create_tables_if_needed_dag", 
    start_date=datetime(2022, 12, 24),
    default_args=default_args
)    
    
PostgresOperator(
    task_id="create_tables",
    dag=dag,
    postgres_conn_id="redshift",
    sql='create_tables.sql'
)
