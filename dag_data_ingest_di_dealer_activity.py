"""
DI Dealer Activity DAG
    Ingestion and Processing of dealer activity data from dependent sources

    Runs Daily
"""

from airflow import DAG
from airflow_operators.dids_spark_bash_operator import DidsSparkBashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import DSConstants as dsc
import InfraUtils
from InfraUtils import sla_missed_slack_alert, task_fail_slack_alert
from StartSparkCluster import CreateSparkCluster

params_1 = {
    "TheClusterName":         'data_ingest_di_dealer_activity',
    "TheNumberOfNodes":       1,
    "TheNodeTypePrefix":      "",
    "TheInstanceRequestType": "ondemand",
    "TheInstanceMinCPUs":     8,
    "TheInstanceMinmem":      32,
    "TheVolSize":             100,
    "TheTimeout":             120,
    "ThePrice":               None,
}

default_args = {
    'owner':               'ubuntu',
    'depends_on_past':     False,
    'start_date':          datetime(2021, 1, 1),
    'on_failure_callback': task_fail_slack_alert,
    'retries':             3,
    'retry_delay':         timedelta(minutes=1),
    'sla':                 timedelta(minutes=30),
}

# Initialize the DAG
schedule_interval = "0 8 * * *"
# schedule_interval = None
with DAG(dag_id='di_dealer_activity', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=sla_missed_slack_alert) as dag:
    # dag.doc_md = (
    #     "<b><h3> Data Ingest DI DEALER ACTIVITY</h3></b>"
    #     "<b>Summary:</b> This DAG contains jobs that import, and post-process DI DEALER ACTIVITY data.<br>"
    #     "<b>Note:</b> For script/repo information about a specific task, click on the task and go to 'Rendered'.<br>"
    #     "View the repositories in GitHub for project-specific information."
    # )

    queue_name_1 = params_1['TheClusterName']
    number_of_nodes_1 = params_1['TheNumberOfNodes']

    spark_submit_command_1 = InfraUtils.generate_start_spark_command(driver_memory='1G', executor_memory='3G')

    #####################################
    # Standard Tasks
    #####################################
    create_cluster_1_task = PythonOperator(
        task_id=f'create_{queue_name_1}',
        python_callable=CreateSparkCluster,
        op_kwargs=params_1)

    kill_cluster_1_task = DidsSparkBashOperator(
        task_id='kill_cluster_1_task',
        bash_command=f'{dsc.CODE_ROOT}/di-awsauto/KillSparkClusterNow.py {queue_name_1}')

    #####################################
    # Ingest Tasks
    #####################################
    process_conversations_dailies_raw_task = DidsSparkBashOperator(
        task_id='process_conversations_dailies_raw',
        bash_command=f'{spark_submit_command_1} {dsc.CODE_ROOT}/di-data_ingest_di_dealer_activity/process_conversations_dailies_raw.py',
        queue=queue_name_1)

    process_online_shopper_dailies_task = DidsSparkBashOperator(
        task_id='process_online_shopper_dailies',
        bash_command=f'{spark_submit_command_1} {dsc.CODE_ROOT}/di-data_ingest_di_dealer_activity/process_online_shopper_dailies.py',
        queue=queue_name_1)

    #####################################
    # Define the workflow
    #####################################
    create_cluster_1_task >> [
        process_conversations_dailies_raw_task,
        process_online_shopper_dailies_task,
    ] >> kill_cluster_1_task

