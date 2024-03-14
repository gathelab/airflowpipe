"""
Template DAG
    1) Launch cluster
    2) Wait until cluster is live
    3) Read Latest Rox events
    4) Kill cluster
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import InfraUtils
from StartSparkCluster import CreateSparkCluster

params = {
    "TheClusterName":         'dev_template_dag',
    "TheNumberOfNodes":       1,
    "TheNodeTypePrefix":      "r5.",
    "TheInstanceRequestType": "spot",
    "TheInstanceMinCPUs":     7,
    "TheInstanceMinmem":      60,
    "TheVolSize":             100,
    "TheTimeout":             120,
    "ThePrice":               None,
    "TheAvailabilityZone":    'us-east-1f',
    # "TheAMIPrefix":           "devspark",  # Defaults to DevSpark and PrdSpark depending on environment
}

default_args = {
    'owner':           'ubuntu',
    'depends_on_past': False,
    'start_date':      datetime(2018, 3, 1),
    'retries':         1,
    'retry_delay':     timedelta(minutes=5),
    # 'on_failure_callback': InfraUtils.task_fail_slack_alert,
    # 'sla':                 timedelta(hours=6),
}

# Initialize the DAG
# schedule_interval = "@once"
schedule_interval = None
with DAG(dag_id='template_dag', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=InfraUtils.sla_missed_slack_alert) as dag:
    queue_name = params['TheClusterName']
    number_of_nodes = params['TheNumberOfNodes']
    default_spark_submit_command = InfraUtils.generate_start_spark_command(driver_memory='4G', executor_memory='7G')

    # Define the tasks
    create_cluster_task = PythonOperator(
        task_id=f'create_{queue_name}',
        python_callable=CreateSparkCluster,
        op_kwargs=params)

    wait_for_cluster_task = BashOperator(
        task_id='wait_for_cluster_task',
        bash_command=f'/home/ubuntu/pycode_di/awsauto/GetSparkStatus.py {queue_name} {number_of_nodes}',
        retries=15*number_of_nodes,
        retry_delay=timedelta(seconds=30))

    read_rox_task = BashOperator(
        task_id='read_latest_rox_events',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/roxanne/ReadLastRoxEvents.py',
        queue=queue_name)

    custom_read_rox_command_1 = InfraUtils.generate_start_spark_command(driver_memory='5G')
    custom_read_rox_task_1 = BashOperator(
        task_id='custom_read_latest_rox_events_1',
        bash_command=f'{custom_read_rox_command_1} /home/ubuntu/pycode_di/roxanne/ReadLastRoxEvents.py',
        queue=queue_name)

    custom_read_rox_command_2 = InfraUtils.generate_start_spark_command(
        driver_memory='5G',
        executor_memory='7G',
        conf_dict={
            'spark.cores.max': 80
        }
    )
    custom_read_rox_task_2 = BashOperator(
        task_id='custom_read_latest_rox_events_2',
        bash_command=f'{custom_read_rox_command_2} /home/ubuntu/pycode_di/roxanne/ReadLastRoxEvents.py',
        queue=queue_name)

    kill_cluster_task = BashOperator(
        task_id='kill_cluster_task',
        bash_command='/home/ubuntu/pycode_di/awsauto/KillSparkClusterNow.py ' + queue_name)

    # Define the workflow
    create_cluster_task >> wait_for_cluster_task >> [read_rox_task, custom_read_rox_task_1,
                                                     custom_read_rox_task_2] >> kill_cluster_task
