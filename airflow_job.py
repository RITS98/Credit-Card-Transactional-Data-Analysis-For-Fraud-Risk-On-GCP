from datetime import datetime, timedelta
import uuid

# Airflow core DAG and operator imports
from airflow import DAG
from airflow.providers.google.cloud.operators.dataproc import DataprocCreateBatchOperator
from airflow.providers.google.cloud.sensors.gcs import GCSObjectsWithPrefixExistenceSensor
from airflow.providers.google.cloud.transfers.gcs_to_gcs import GCSToGCSOperator
from airflow.utils.trigger_rule import TriggerRule

# -----------------------
# DAG DEFAULT ARGUMENTS
# -----------------------
# These default arguments apply to all tasks unless overridden
default_args = {
    'owner': 'ritayan',                            # Owner of the DAG
    'depends_on_past': False,                      # DAG run is independent of previous runs
    'retries': 1,                                  # Number of times to retry on failure
    'retry_delay': timedelta(minutes=5),           # Wait time between retries
    'start_date': datetime(2025, 2, 7),            # Start date for the DAG
}

# -----------------------
# DEFINE DAG
# -----------------------
# DAG is defined with no schedule (manual trigger only)
with DAG(
    dag_id="credit_card_transactions_dataproc_dag",    # Unique identifier for the DAG
    default_args=default_args,                         # Apply default arguments
    schedule_interval=None,                            # No automatic schedule
    catchup=False,                                     # Don’t run past DAG runs on start
) as dag:

    # -------------------------------------
    # CONSTANTS: GCS Bucket & File Prefixes
    # -------------------------------------
    gcs_bucket = "credit-card-data-analysis"           # GCS bucket name
    file_pattern = "transactions/transactions_"        # Pattern to identify new files
    source_prefix = "transactions/"                    # Source directory in GCS
    archive_prefix = "archive/"                        # Archive directory in GCS

    # --------------------------------------------------------
    # TASK 1: GCS Sensor - Wait for Matching JSON Files to Arrive
    # --------------------------------------------------------
    # Waits for new file(s) with the given prefix to appear in GCS
    file_sensor = GCSObjectsWithPrefixExistenceSensor(
        task_id="check_json_file_arrival",
        bucket=gcs_bucket,                             # Bucket to monitor
        prefix=file_pattern,                           # Prefix to match files
        timeout=600,                                   # Timeout after 10 minutes
        poke_interval=30,                              # Check every 30 seconds
        mode="poke",                                   # Poke mode waits synchronously
    )

    # --------------------------------------------------------
    # TASK 2: Dataproc Batch Job - Submit PySpark Job to Dataproc Serverless
    # --------------------------------------------------------
    # Create a unique ID for each Dataproc batch job
    batch_id = f"credit-card-batch-{str(uuid.uuid4())[:8]}"  # Short unique ID

    # Configuration for the PySpark job submission
    batch_details = {
        "pyspark_batch": {
            # Main PySpark file to run, stored in GCS
            "main_python_file_uri": "gs://credit-card-data-analysis-ritayan/spark_job/spark_job.py"
        },
        "runtime_config": {
            "version": "2.2",                          # Dataproc Serverless runtime version
        },
        "environment_config": {
            "execution_config": {
                "service_account": "70622048644-compute@developer.gserviceaccount.com",  # IAM service account
                "network_uri": "projects/psyched-service-442305-q1/global/networks/default",  # VPC
                "subnetwork_uri": "projects/psyched-service-442305-q1/regions/us-central1/subnetworks/default",  # Subnet
            }
        },
    }

    # Actual operator to create the Dataproc Serverless batch job
    pyspark_task = DataprocCreateBatchOperator(
        task_id="run_credit_card_processing_job",
        batch=batch_details,                          # Job configuration
        batch_id=batch_id,                            # Unique batch ID
        project_id="psyched-service-442305-q1",       # GCP project ID
        region="us-central1",                         # Dataproc region
        gcp_conn_id="google_cloud_default",           # Airflow connection to GCP
    )

    # --------------------------------------------------------
    # TASK 3: Move Processed Files from Transactions Folder to Archive
    # --------------------------------------------------------
    # After processing completes, move the input files to the archive folder
    move_files_to_archive = GCSToGCSOperator(
        task_id="move_files_to_archive",
        source_bucket=gcs_bucket,                     # Source GCS bucket
        source_object=source_prefix,                  # Source folder prefix
        destination_bucket=gcs_bucket,                # Destination is same bucket
        destination_object=archive_prefix,            # Archive folder prefix
        move_object=True,                             # Move instead of copy
        trigger_rule=TriggerRule.ALL_SUCCESS,         # Only runs if upstream tasks succeed
    )

    # --------------------------------------------------------
    # DAG TASK DEPENDENCIES / EXECUTION FLOW
    # --------------------------------------------------------
    # 1. Wait for new file
    # 2. Submit Dataproc job
    # 3. Move processed file to archive
    file_sensor >> pyspark_task >> move_files_to_archive
