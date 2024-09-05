from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryExecuteQueryOperator
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import pandas as pd
from google.cloud import bigquery

# Define default arguments
default_args = {
    'owner': 'airflow_SS',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
}

def read_bq_data(**kwargs):
    # Create a BigQuery client
    client = bigquery.Client()

    # Define the query
    query = """
    SELECT * FROM `aceinternal-2ed449d3.sandbox_ss_eu.dev_result_3`
    Limit 100
    """
    query2 = """
    SELECT count(*) as row_count
    FROM `aceinternal-2ed449d3.sandbox_ss_eu.dev_result_3`

    """


    # Run the query and get the result as a DataFrame
    df = client.query(query).to_dataframe()
    query_job = client.query(query2)

    results = query_job.result()

    # Print the result
    for row in results:
        print(f"Row count: {row.row_count}")
    # Save the DataFrame to a local file
    # df.to_csv('/opt/airflow/data/bq_data.csv', index=False)
    print("Data has been written to /opt/airflow/data/bq_data.csv")
 
    

with DAG(
    dag_id='bq_to_local_dag',
    default_args=default_args,
    description='A DAG to read data from BigQuery and save it to a local file',
    schedule_interval='@daily',
) as dag:

    # Task to run the BigQuery SQL query
    bq_query_task = PythonOperator(
        task_id='read_bq_data',
        python_callable=read_bq_data,
        provide_context=True
    )

    bq_query_task
