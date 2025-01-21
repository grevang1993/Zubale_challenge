from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.providers.google.cloud.operators.bigquery import BigQueryInsertJobOperator
from airflow.operators.python import PythonOperator
from datetime import datetime

# Extract Data from PostgreSQL
def extract_postgres_data(**kwargs):
    pg_hook = PostgresHook(postgres_conn_id='postgres_default')
    conn = pg_hook.get_conn()
    cursor = conn.cursor()
    cursor.execute("""
        SELECT o.created_date, o.id AS order_id, p.name AS product_name, o.quantity,
               (o.quantity * p.price) AS total_price
        FROM orders o
        JOIN products p ON o.product_id = p.id;
    """)
    rows = cursor.fetchall()
    kwargs['ti'].xcom_push(key='extracted_data', value=rows)

# Transform and Load into BigQuery
def transform_and_load_to_bigquery(**kwargs):
    from google.cloud import bigquery

    # Initialize BigQuery client
    client = bigquery.Client()
    
    # Get extracted data
    rows = kwargs['ti'].xcom_pull(key='extracted_data', task_ids='extract_postgres_data')

    # Transform data if needed
    transformed_rows = [
        {"order_created_date": row[0], "order_id": row[1], "product_name": row[2],
         "quantity": row[3], "total_price": row[4]} for row in rows
    ]

    # Define BigQuery table
    table_id = "project_example.dataset.orders_data"

    # Load data to BigQuery
    errors = client.insert_rows_json(table_id, transformed_rows)
    if errors:
        raise Exception(f"BigQuery insert errors: {errors}")

# Define the DAG
with DAG(
    dag_id='postgres_to_bigquery_pipeline',
    start_date=datetime(2025, 1, 1),
    schedule_interval='@daily',
    catchup=False,
) as dag:

    extract_data = PythonOperator(
        task_id='extract_postgres_data',
        python_callable=extract_postgres_data
    )

    load_data = PythonOperator(
        task_id='transform_and_load_to_bigquery',
        python_callable=transform_and_load_to_bigquery
    )

    extract_data >> load_data