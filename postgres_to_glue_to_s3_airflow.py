from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.operators.glue import GlueJobOperator
from datetime import datetime, timedelta

# Import our custom tasks from separate files
from tasks.db_tasks import check_postgres_connection
from tasks.s3_tasks import verify_s3_upload

# Configuration
POSTGRES_CONN_ID = 'aurora_postgres_aws'
AWS_GLUE_JOB_NAME = 'my_glue_etl_job'
S3_OUTPUT_BUCKET = 'mwaa-artifacts-7'
S3_OUTPUT_PREFIX = 'glue_output/'
IAM_ROLE_NAME = 'mwaa-7-MWAA-ExecutionRole' 

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'postgres_to_s3_glue_workflow',
    default_args=default_args,
    schedule_interval=None,
    catchup=False
) as dag:

    # --- Task 1: Connect to Postgres (Validation) ---
    # We verify the DB is up before triggering the heavy Glue job
    task_validate_db = PythonOperator(
        task_id='validate_postgres_connection',
        python_callable=check_postgres_connection,
        op_kwargs={'connection_id': POSTGRES_CONN_ID}
    )

    # --- Task 2: Run Select & Upload (The Glue Job) ---
    # This operator triggers the AWS Glue Job defined in step 1
    task_run_glue_etl = GlueJobOperator(
        task_id='run_glue_etl_job',
        job_name=AWS_GLUE_JOB_NAME,
        script_location='s3://mwaa-artifacts-7/glue_scripts/glue_etl_script.py',
        s3_bucket=S3_OUTPUT_BUCKET,
        iam_role_name=IAM_ROLE_NAME,
        create_job_kwargs={
            "GlueVersion": "3.0", 
            "WorkerType": "G.1X", 
            "NumberOfWorkers": 2
        },
        script_args={
            '--s3_output_path': f"s3://{S3_OUTPUT_BUCKET}/{S3_OUTPUT_PREFIX}",
            '--db_connection_name': 'aurora_postgres_aws'
        },
        aws_conn_id='aws_default',
        region_name='ap-south-1',
        wait_for_completion=True
    )

    # --- Task 3: Put output in S3 (Verification) ---
    # We verify the data actually landed in S3
    task_verify_s3 = PythonOperator(
        task_id='verify_s3_data',
        python_callable=verify_s3_upload,
        op_kwargs={
            'bucket_name': S3_OUTPUT_BUCKET,
            'prefix': S3_OUTPUT_PREFIX
        }
    )

    # --- Dependency Flow ---
    task_validate_db >> task_run_glue_etl >> task_verify_s3