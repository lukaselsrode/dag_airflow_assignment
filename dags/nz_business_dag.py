from airflow import DAG
from airflow.utils.dates import days_ago
from nz_business_pipeline import stage_files_in_gcs, ingest_data_from_storage_to_bigquery
from airflow.operators.python import PythonOperator

default_args = {
    'owner': 'Lukas Elsrode',
    'start_date': days_ago(1),
}

dag = DAG(
    'nz_business_dag',
    default_args=default_args,
    description='DAG for staging files in GCS and ingesting data into BigQuery',
    schedule_interval='@daily',
)

task_stage_files_in_gcs = PythonOperator(
    task_id='stage_files_in_gcs',
    python_callable=stage_files_in_gcs,
    dag=dag,
)

task_ingest_data = PythonOperator(
    task_id='ingest_data_from_storage_to_bigquery',
    python_callable=ingest_data_from_storage_to_bigquery,
    dag=dag,
)

task_stage_files_in_gcs >> task_ingest_data