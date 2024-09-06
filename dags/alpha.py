from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from datetime import datetime

default_args = {
    'owner': 'airflow_SS',
    'start_date': datetime(2024, 1, 1)
    
}

with DAG('alpha',
         default_args=default_args,
         schedule_interval='@daily',
         catchup=False) as dag:

    read_query = BigQueryExecuteQueryOperator(
        task_id='read_from_bigquery',
        sql='SELECT * FROM `aceinternal-2ed449d3.sandbox_ss_eu.dev_result_3` LIMIT 10',
        use_legacy_sql=False
    )

    read_query
