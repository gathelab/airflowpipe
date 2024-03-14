"""
Ingestion of sales data from the s3 for FTP server

Runs Daily
"""

from airflow import DAG
from airflow_operators.dids_spark_bash_operator import DidsSparkBashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import InfraUtils
from StartSparkCluster import CreateSparkCluster


params_1 = {
    "TheClusterName":         'data_ingest_test_shalabh',
    "TheNumberOfNodes":       1,
    "TheNodeTypePrefix":      "r5.",
    "TheInstanceRequestType": "ondemand",
    "TheInstanceMinCPUs":     8,
    "TheInstanceMinmem":      32,
    "TheVolSize":             100,
    "TheTimeout":             120,
    "ThePrice":               None,
    "TheAvailabilityZone":    'us-east-1f',
}

default_args = {
    'owner':               'ubuntu',
    'depends_on_past':     False,
    'start_date':          datetime(2019, 5, 28),
    'on_failure_callback': InfraUtils.task_fail_slack_alert,
    'retries':             1,
    'sla':                 timedelta(hours=1),
}

# Initialize the DAG
schedule_interval = "10 8 * * *"  # Staggered slightly to make the scheduler's job easier
# schedule_interval = None
with DAG(dag_id='data_ingest_test_shalabh', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=InfraUtils.sla_missed_slack_alert) as dag:
    # dag.doc_md = (
    #     "<b><h3> Data Ingest Test Shalabh</h3></b>"
    #     "<b>Related DAG(s):</b> N/A<br><br>"
    #     "<b>Note:</b> For script/repo information about a specific task, click on the task and go to 'Rendered'.<br>"
    #     "View the repositories in BitBucket for project-specific information."
    # )
    queue_name_1 = params_1['TheClusterName']
    number_of_nodes_1 = params_1['TheNumberOfNodes']

    spark_submit_command_1 = InfraUtils.generate_start_spark_command(driver_memory='5G', executor_memory='7G')

    #####################################
    # Standard Tasks
    #####################################
    create_cluster_1_task = PythonOperator(
        task_id=f'create_{queue_name_1}',
        python_callable=CreateSparkCluster,
        op_kwargs=params_1)

    wait_for_cluster_1_task = DidsSparkBashOperator(
        task_id='wait_for_cluster_1_task',
        bash_command=f'/home/ubuntu/pycode_di/awsauto/GetSparkStatus.py {queue_name_1} {number_of_nodes_1}',
        retries=15*number_of_nodes_1,
        retry_delay=timedelta(seconds=30))

    kill_cluster_1_task = DidsSparkBashOperator(
        task_id='kill_cluster_1_task',
        bash_command=f'/home/ubuntu/pycode_di/awsauto/KillSparkClusterNow.py {queue_name_1}')

    #####################################
    # Ingest Tasks
    #####################################
    ingest_sales_from_s3_task = DidsSparkBashOperator(
        task_id='ingest_sales_from_s3',
        bash_command=f'{spark_submit_command_1} /home/ubuntu/pycode_di/shalabh_test/ingest_sales_from_s3.py',
        queue=queue_name_1)

    #####################################
    # Define the workflow
    #####################################
    create_cluster_1_task >> wait_for_cluster_1_task >> [
        ingest_sales_from_s3_task,
    ] >> kill_cluster_1_task
