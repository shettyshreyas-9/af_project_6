import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from google.oauth2 import service_account

# # Set path to your service account key
# SERVICE_ACCOUNT_KEY_PATH = '/opt/airflow/keys/gcp-test.json'
# os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY_PATH


try:
    SERVICE_ACCOUNT_KEY_PATH = '/opt/airflow/keys/gcp-test.json'
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = SERVICE_ACCOUNT_KEY_PATH
    print(f"Service account key path set to: {SERVICE_ACCOUNT_KEY_PATH}")
except Exception as e:
    print(f"An error occurred while setting the service account key path: {e}")


# Function to run the Beam pipeline
def run_beam_pipeline(**kwargs):
    # Set your project ID and bucket names
    project_id = 'aceinternal-2ed449d3'
    raw_data_bucket = 'gs://sn_insights_test/input/OnlineRetail.csv'
    dataset_id = 'sandbox_ss_eu'
    table_id = 'processed_dfj'

    # Define the pipeline options
    options = PipelineOptions([
        '--project=' + project_id,
        '--job_name=etl-pipeline-job-2',
        '--staging_location=gs://sn_insights_test/staging',
        '--temp_location=gs://sn_insights_test/temp',
        '--region=europe-west2',
        # '--runner=DataflowRunner'  # Explicitly specify DataflowRunner
    ])

    # Beam pipeline
    with beam.Pipeline(options=options) as p:
        (
            p
            | 'ReadFromText' >> beam.io.ReadFromText(raw_data_bucket, skip_header_lines=1,coder=beam.coders.BytesCoder())
            | 'Decode' >> beam.Map(lambda x: x.decode('ISO-8859-1').split(','))
            | 'Filter' >> beam.Filter(lambda x: x[1] in ['85123A', '71053', '84406B', '84029G'])
            # | 'FormatForBigQuery' >> beam.Map(lambda x: {'item_code': x[1], 'total_amount': float(x[5])})
            | 'FormatForBigQuery' >> beam.Map(lambda x: (x[1], float(x[5])))
            | 'SumByKey' >> beam.CombinePerKey(sum)  # Aggregate values
            | 'FormatForBigQueryOutput' >> beam.Map(lambda kv: {'item_code': kv[0], 'total_amount': kv[1]})
            | 'WriteToBigQuery' >> beam.io.WriteToBigQuery(
                f"{project_id}:{dataset_id}.{table_id}",
                schema={
                    'fields': [
                        {'name': 'item_code', 'type': 'STRING', 'mode': 'REQUIRED'},
                        {'name': 'total_amount', 'type': 'FLOAT', 'mode': 'REQUIRED'}
                    ]
                },
                write_disposition=beam.io.BigQueryDisposition.WRITE_APPEND,
                create_disposition=beam.io.BigQueryDisposition.CREATE_IF_NEEDED
            )
        )

if __name__ == '__main__':
    run_beam_pipeline()
