from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
import os

# Define default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 9, 6),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



# Define the DAG
with DAG(
    dag_id='apache_beam_etl_pipeline',
    default_args=default_args,
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:
    
    
    

    # run_beam_pipeline6 = BashOperator(
    #     task_id='run_beam_pipeline6',
    #     bash_command='python3 /opt/airflow/dags/etl_pipe_1.py',  # Update path accordingly
    # )

    run_beam_pipeline7 = BashOperator(
        task_id='run_beam_pipeline7',
        bash_command='python3 /opt/airflow/beam_scripts/etl_pipe_1.py',  # Update path accordingly
    )


    list_dir = BashOperator(
        task_id='list_dir',
        bash_command='ls -l',  # Update path accordingly
    )

    # Task to check the working directory
    check_dir_task = BashOperator(
        task_id='check_working_directory',
        bash_command='pwd'
    )

    
    www = BashOperator(
        task_id='www',
        bash_command='pip install apache-beam[gcp]'
    )

    # www = BashOperator(
    #     task_id='www',
    #     # bash_command='python3 -m venv /opt/airflow/venv'
    #     bash_command='source /opt/airflow/venv/bin/activate'
    # )

    www >> run_beam_pipeline7

    
