from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import dataflow_v1beta3

# Set up the Dataflow client
def run_dataflow_job(**kwargs):
    project_id = 'aceinternal-2ed449d3'
    region = 'europe-west2'
    template_path = 'gs://sn_insights_test/templates/etl-pipeline-template'
    
    # Dataflow job parameters
    parameters = {
        'input': 'gs://sn_insights_test/input/OnlineRetail.csv',
        'output': 'gs://sn_insights_test/output',
    }

    # Dataflow client
    client = dataflow_v1beta3.FlexTemplatesServiceClient()
    
    # Create the request to launch the template
    launch_parameters = dataflow_v1beta3.LaunchFlexTemplateParameter(
        job_name='etl-pipeline-job',
        container_spec_gcs_path=template_path,
        parameters=parameters
    )

    launch_request = dataflow_v1beta3.LaunchFlexTemplateRequest(
        project_id=project_id,
        location=region,
        launch_parameters=launch_parameters  # Use the correct structure here
    )

    # Launch the Dataflow job
    response = client.launch_flex_template(request=launch_request)  # Pass the entire request object
    print(f'Job launched: {response.job.id}')

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'dataflow_dag',
    default_args=default_args,
    description='A DAG to trigger Dataflow jobs',
    schedule_interval='@daily',  # Change this to your preferred schedule
)

# Define the task
run_dataflow = PythonOperator(
    task_id='run_dataflow',
    python_callable=run_dataflow_job,
    provide_context=True,
    dag=dag,
)

run_dataflow