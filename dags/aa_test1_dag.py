# from airflow import DAG
# from datetime import datetime
# from airflow.utils.dates import days_ago
# from airflow.operators.bash import BashOperator

# import subprocess
# subprocess.run(["pip install apache-airflow-providers-apache-beam"], check=True)


# from airflow.providers.apache.beam.operators.beam import BeamRunPythonPipelineOperator


# default_args = {
#     'owner': 'airflow_SS',
#     'depends_on_past': False,
#     'start_date': days_ago(1),
#     'retries': 1,
# }
 
# with DAG(
#     'example_dataflow_dag',
#     default_args=default_args,
#     schedule_interval=None,
#     catchup=False,
# ) as dag:
    

#     # Define the Dataflow job using BeamRunPythonPipelineOperator
#     run_dataflow_job = BeamRunPythonPipelineOperator(
#         task_id='run_dataflow_job',
#         py_file='gs://sn_insights_test/af_project_6/beam_scripts/etl_pipe_1.py',  # Path to your Python script in GCS
#         py_options=[],
#         job_name='dataflow-python-job',
#         location='europe-west2',  # Specify the location
#         project_id='aceinternal-2ed449d3',  # Your GCP project ID
#         py_requirements=['apache-beam[gcp]==2.25.0'],  # Specify any required Python packages
#         wait_until_finished=False,  # Set to True if you want to wait for the job to finish
#     )

#     run_dataflow_job
    
