"""
Ingestion and post processing of Ford Direct Reporting

Runs Daily
"""

from airflow import DAG
from airflow_operators.dids_spark_bash_operator import DidsSparkBashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import InfraUtils
from StartSparkCluster import CreateSparkCluster

params_1 = {
    "TheClusterName":         'ford_direct_reporting',
    "TheNumberOfNodes":       1,
    "TheNodeTypePrefix":      "r5.",
    "TheInstanceRequestType": "ondemand",
    "TheInstanceMinCPUs":     16,
    "TheInstanceMinmem":      120,
    "TheVolSize":             100,
    "TheTimeout":             120,
    "ThePrice":               None,
    "TheAvailabilityZone":    'us-east-1f',
}

default_args = {
    'owner':               'ubuntu',
    'depends_on_past':     False,
    'start_date':          datetime(2020, 1, 10),
    'on_failure_callback': InfraUtils.task_fail_slack_alert,
    'sla':                 timedelta(hours=4),
    'retries':             1,
    'retry_delay':         timedelta(minutes=5),
}


# Because this is a monthly DAG, it will look for a DAG Run from the eighth day of last month by default
# We're referencing daily DAGs, so we want the seventh day of the current month instead
def get_fuel_data_execution_date(the_execution_date):
    fuel_execution_date = the_execution_date.replace(hour=10, minute=30, second=0, microsecond=0) + relativedelta(months=1) - relativedelta(days=1)
    return fuel_execution_date


def get_ga_execution_date(the_execution_date):
    ga_execution_date = the_execution_date.replace(hour=8, minute=5, second=0, microsecond=0) + relativedelta(months=1) - relativedelta(days=1)
    return ga_execution_date


def get_bing_and_facebook_execution_date(the_execution_date):
    bing_and_facebook_execution_date = the_execution_date.replace(hour=11, minute=00, second=0, microsecond=0) + relativedelta(months=1) - relativedelta(days=1)
    return bing_and_facebook_execution_date


def get_google_adwords_execution_date(the_execution_date):
    google_adwords_execution_date = the_execution_date.replace(hour=13, minute=00, second=0, microsecond=0) + relativedelta(months=1) - relativedelta(days=1)
    return google_adwords_execution_date


def get_google_dv360_insertion_execution_date(the_execution_date):
    google_dv360_execution_date = the_execution_date.replace(hour=11, minute=00, second=0, microsecond=0) + relativedelta(months=1) - relativedelta(days=1)
    return google_dv360_execution_date

# Initialize the DAG
schedule_interval = "0 14 8 * *"
# schedule_interval = None
with DAG(dag_id='ford_direct_reporting', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=InfraUtils.sla_missed_slack_alert) as dag:
    # dag.doc_md = (
    #     "<b><h3> Ford Direct Reporting</h3></b>"
    #     "<b>Summary:</b> This DAG contains jobs that create reports for ford-direct and publish it to their FTP Servers.<br>"
    #     "It compiles all the Campaign level stats from Bing & Google AdWords and AdSet level stats from Facebook for ford direct clients.<br>"
    #     "It then creates 3 separate files i.e. Social, Search and Display. Then it publishes those reports to their FTP Servers in CSV format.<br><br>"
    #     "<b>Repository(-ies):</b> [ford_direct_reporting](https://bitbucket.org/dealerinspire/ford_direct_reporting/src/master/)<br><br>"
    #     "<b>Note:</b> For script/repo information about a specific task, click on the task and go to 'Rendered'.<br>"
    #     "View the repositories in BitBucket for project-specific information."
    # )
    queue_name_1 = params_1['TheClusterName']
    number_of_nodes_1 = params_1['TheNumberOfNodes']

    default_spark_submit_command = InfraUtils.generate_start_spark_command(driver_memory='5G', executor_memory='7G')

    #####################################
    # Standard Tasks
    #####################################
    create_cluster_1_task = PythonOperator(
        task_id=f'create_{queue_name_1}',
        python_callable=CreateSparkCluster,
        op_kwargs=params_1)

    wait_for_cluster_1_task = DidsSparkBashOperator(
        task_id='wait_for_cluster_task',
        bash_command=f'/home/ubuntu/pycode_di/awsauto/GetSparkStatus.py {queue_name_1} {number_of_nodes_1}',
        retries=15 * number_of_nodes_1,
        retry_delay=timedelta(seconds=30))

    kill_cluster_1_task = DidsSparkBashOperator(
        task_id='kill_cluster_task',
        bash_command=f'/home/ubuntu/pycode_di/awsauto/KillSparkClusterNow.py {queue_name_1}')

    #####################################
    # Import Tasks
    #####################################
    # This job depends on daily ingest of fuel jobs to be completed
    wait_for_fuel_clients_sensor = ExternalTaskSensor(
        task_id='wait_for_fuel_clients',
        external_dag_id='data_ingest_fuel',
        external_task_id='fuel_clients_ford_direct',
        execution_date_fn=get_fuel_data_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    # This job depends on daily ingest of facebook adset dailies job to be completed
    wait_for_facebook_adsets_sensor = ExternalTaskSensor(
        task_id='wait_for_facebook_adsets',
        external_dag_id='data_ingest_facebook',
        external_task_id='process_facebook_adset_dailies',
        execution_date_fn=get_bing_and_facebook_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    # This job depends on daily ingest of google analytics goal dailies ford direct job to be completed
    wait_for_google_analytics_goals_dailies_ford_direct_sensor = ExternalTaskSensor(
        task_id='wait_for_google_analytics_goals_dailies_ford_direct',
        external_dag_id='data_ingest_google_analytics',
        external_task_id='google_analytics_goals_dailies_ford_direct',
        execution_date_fn=get_ga_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    # This job depends on daily ingest of bing campaigns dailies jobs to be completed
    wait_for_bing_campaigns_sensor = ExternalTaskSensor(
        task_id='wait_for_bing_campaigns',
        external_dag_id='data_ingest_bing',
        external_task_id='process_bing_campaign_dailies',
        execution_date_fn=get_bing_and_facebook_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    # This job depends on daily ingest of google adwords campaigns dailies job to be completed
    wait_for_google_adwords_campaigns_sensor = ExternalTaskSensor(
        task_id='wait_for_google_adwords_campaigns',
        external_dag_id='data_ingest_google_adwords',
        external_task_id='google_adwords_campaign_dailies_prizm',
        execution_date_fn=get_google_adwords_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    # This job depends on daily ingest of google dv360 insertion order dailies job to be completed
    wait_for_google_dv360_insertion_sensor = ExternalTaskSensor(
        task_id='wait_for_google_dv360_insertion',
        external_dag_id='data_ingest_google_dv360',
        external_task_id='process_google_dv360_insertion_order_dailies',
        execution_date_fn=get_google_dv360_insertion_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    #####################################
    # Process Tasks
    #####################################
    # Ford-Direct Social
    process_ford_direct_social_task = DidsSparkBashOperator(
        task_id='process_ford_direct_social',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/process_ford_direct_social.py',
        queue=queue_name_1)

    # Ford-Direct Search
    process_ford_direct_search_task = DidsSparkBashOperator(
        task_id='process_ford_direct_search',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/process_ford_direct_search.py',
        queue=queue_name_1)

    # Ford-Direct Display
    process_ford_direct_display_task = DidsSparkBashOperator(
        task_id='process_ford_direct_display',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/process_ford_direct_display.py',
        queue=queue_name_1)

    # Ford-Direct Premium Display
    process_ford_direct_premium_display_task = DidsSparkBashOperator(
        task_id='process_ford_direct_premium_display',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/process_ford_direct_premium_display.py',
        queue=queue_name_1)

    # Ford-Direct Fuel IMV
    process_ford_direct_fuel_imv_task = DidsSparkBashOperator(
        task_id='process_ford_direct_fuel_imv',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/process_ford_direct_fuel_imv.py',
        queue=queue_name_1)
    #####################################
    # Publish Tasks
    #####################################

    # Ford-Direct CSV
    ford_direct_parquet_to_csv_task = DidsSparkBashOperator(
        task_id='ford_direct_parquet_to_csv',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/ford_direct_parquet_to_csv.py',
        queue=queue_name_1)
    # Ford-Direct premium display & Fuel IMV CSV
    ford_direct_premium_parquet_to_csv_task = DidsSparkBashOperator(
        task_id='ford_direct_premium_parquet_to_csv',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/ford_direct_premium_parquet_to_csv.py',
        queue=queue_name_1)


    # Ford-Direct Reports to FTP
    publish_ford_direct_csv_to_ftp_task = DidsSparkBashOperator(
        task_id='publish_ford_direct_csv_to_ftp',
        bash_command=f'{default_spark_submit_command} /home/ubuntu/pycode_di/ford_direct_reporting/publish_ford_direct_csv_to_ftp.py',
        queue=queue_name_1)

    #####################################
    # Define the workflow
    #####################################

    [wait_for_fuel_clients_sensor, wait_for_facebook_adsets_sensor, wait_for_bing_campaigns_sensor, wait_for_google_adwords_campaigns_sensor, wait_for_google_analytics_goals_dailies_ford_direct_sensor, wait_for_google_dv360_insertion_sensor] >> create_cluster_1_task

    create_cluster_1_task >> wait_for_cluster_1_task >> process_ford_direct_social_task
    create_cluster_1_task >> wait_for_cluster_1_task >> process_ford_direct_search_task
    create_cluster_1_task >> wait_for_cluster_1_task >> process_ford_direct_display_task
    create_cluster_1_task >> wait_for_cluster_1_task >> process_ford_direct_premium_display_task
    create_cluster_1_task >> wait_for_cluster_1_task >> process_ford_direct_fuel_imv_task

    process_ford_direct_social_task >> ford_direct_parquet_to_csv_task
    process_ford_direct_search_task >> ford_direct_parquet_to_csv_task
    process_ford_direct_display_task >> ford_direct_parquet_to_csv_task
    process_ford_direct_premium_display_task >> ford_direct_premium_parquet_to_csv_task
    process_ford_direct_fuel_imv_task >> ford_direct_premium_parquet_to_csv_task

    ford_direct_parquet_to_csv_task >> publish_ford_direct_csv_to_ftp_task
    ford_direct_premium_parquet_to_csv_task >> publish_ford_direct_csv_to_ftp_task

    publish_ford_direct_csv_to_ftp_task >> kill_cluster_1_task
