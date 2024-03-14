"""
Ingestion and post processing of Google Adwords Data
Runs Daily
"""

from airflow import DAG
from airflow_operators.dids_spark_bash_operator import DidsSparkBashOperator
from airflow.operators.python_operator import PythonOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
import DSConstants as dsc
import InfraUtils
from StartSparkCluster import CreateSparkCluster

params_cluster1 = {
    "TheClusterName":            'data_ingest_google_adwords_1',
    "TheNumberOfNodes":          4,
    "TheNodeTypePrefix":         "r5.",
    "TheInstanceRequestType":    "ondemand",
    "TheInstanceMinCPUs":        16,
    "TheInstanceMinmem":         120,
    "TheVolSize":                100,
    "TheTimeout":                120,
    "ThePrice":                  None,
    "TheAMIPrefix":           "prd37spark",
    "TheAvailabilityZone":       'us-east-1f',
    "TheNumberOfAirflowWorkers": 2,
}

params_cluster2 = {
    "TheClusterName":            'data_ingest_google_adwords_2',
    "TheNumberOfNodes":          4,
    "TheNodeTypePrefix":         "r5.",
    "TheInstanceRequestType":    "ondemand",
    "TheInstanceMinCPUs":        16,
    "TheInstanceMinmem":         120,
    "TheVolSize":                100,
    "TheTimeout":                120,
    "ThePrice":                  None,
    "TheAMIPrefix":           "prd37spark",
    "TheAvailabilityZone":       'us-east-1f',
    "TheNumberOfAirflowWorkers": 2,
}

default_args = {
    'owner': 'ubuntu',
    'depends_on_past': False,
    'start_date': datetime(2020, 6, 18),
    'on_failure_callback': InfraUtils.task_fail_slack_alert,
    'sla': timedelta(hours=3),
    'retries': 5,
    'retry_delay': timedelta(minutes=5),
    #'retry_exponential_backoff': True,
    'execution_timeout': timedelta(hours=1),
}


def get_dealers_data_execution_date(the_execution_date):
    dag_execution_date = the_execution_date.replace(hour=8, minute=00, second=0, microsecond=0)
    return dag_execution_date


def get_fuel_data_execution_date(the_execution_date):
    dag_execution_date = the_execution_date.replace(hour=10, minute=30, second=0, microsecond=0)
    return dag_execution_date


def get_salesforce_dealer_mapping_data_execution_date(the_execution_date):
    dag_execution_date = the_execution_date.replace(hour=8, minute=10, second=0, microsecond=0)
    return dag_execution_date


# Initialize the DAG
schedule_interval = "0 13 * * *"
# schedule_interval = None
with DAG(dag_id='data_ingest_google_adwords', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=InfraUtils.sla_missed_slack_alert_fuel) as dag:
    # dag.doc_md = (
    #     "<b><h3> Data Ingest google_adwords</h3></b>"
    #     "<b>Summary:</b> This DAG contains jobs that import and post-process Google Adwords data.<br>"
    #     "It compiles a list of google_adwords account IDs from the google_adwords API.<br>"
    #     "It then gets account and campaign level daily/monthly reports and pushes them to prizm.<br><br>"
    #     "<b>Repository(-ies):</b> [data_ingest_google_adwords](https://github.com/cars-data/di-data_ingest_google_adwords)<br><br>"
    #     "<b>Note:</b> For script/repo information about a specific task, click on the task and go to 'Rendered'.<br>"
    #     "View the repositories in GitHub for project-specific information."
    # )
    queue_name_1 = params_cluster1['TheClusterName']
    number_of_nodes_1 = params_cluster1['TheNumberOfNodes']
    queue_name_2 = params_cluster2['TheClusterName']
    number_of_nodes_2 = params_cluster2['TheNumberOfNodes']

    default_spark_submit_command = InfraUtils.generate_start_spark_command(driver_memory='5G', executor_memory='7G')

    publish_job_spark_submit_command = InfraUtils.generate_start_spark_command(
        driver_memory='5G',
        executor_memory='7G',
        conf_dict={
            'spark.cores.max': 12
        }
    )

    publish_keywords_job_spark_submit_command = InfraUtils.generate_start_spark_command(
        driver_memory='5G',
        executor_memory='7G',
        conf_dict={
            'spark.cores.max': 3
        }
    )

    total_cpus_cluster1 = params_cluster1['TheInstanceMinCPUs'] * params_cluster1['TheNumberOfNodes']
    half_cpus_cluster1 = int(total_cpus_cluster1 / 2) + 1

    total_cpus_cluster2 = params_cluster2['TheInstanceMinCPUs'] * params_cluster2['TheNumberOfNodes']
    half_cpus_cluster2 = int(total_cpus_cluster2 / 2) + 1

    default_spark_submit_command_cluster1 = InfraUtils.generate_start_spark_command(
        driver_memory='5G',
        executor_memory='7G',
        conf_dict={
            'spark.cores.max': half_cpus_cluster1
        }
    )
    default_spark_submit_command_cluster2 = InfraUtils.generate_start_spark_command(
        driver_memory='5G',
        executor_memory='7G',
        conf_dict={
            'spark.cores.max': half_cpus_cluster2
        }
    )

    #####################################
    # Standard Tasks
    #####################################
    create_cluster_1_task = PythonOperator(
        task_id=f'create_{queue_name_1}',
        python_callable=CreateSparkCluster,
        op_kwargs=params_cluster1)

    kill_cluster_1_task = DidsSparkBashOperator(
        task_id='kill_cluster_task',
        bash_command=f'{dsc.CODE_ROOT}/di-awsauto/KillSparkClusterNow.py {queue_name_1}')

    create_cluster_2_task = PythonOperator(
        task_id=f'create_{queue_name_2}',
        python_callable=CreateSparkCluster,
        op_kwargs=params_cluster2)

    kill_cluster_2_task = DidsSparkBashOperator(
        task_id='kill_cluster_2_task',
        bash_command=f'{dsc.CODE_ROOT}/di-awsauto/KillSparkClusterNow.py {queue_name_2}')

    # This job depends on daily ingest of multiple tables to be completed
    wait_for_dealers_sensor = ExternalTaskSensor(
        task_id='wait_for_dealers',
        external_dag_id='data_ingest_dashboard',
        external_task_id='dealers',
        execution_date_fn=get_dealers_data_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    wait_for_fuel_clients_to_rox_sites_sensor = ExternalTaskSensor(
        task_id='wait_for_fuel_clients_to_rox_sites',
        external_dag_id='data_ingest_fuel',
        external_task_id='fuel_clients_to_rox_sites',
        execution_date_fn=get_fuel_data_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')

    wait_for_salesforce_accounts_to_dashboard_dealers_mapping_sensor = ExternalTaskSensor(
        task_id='wait_for_salesforce_accounts_to_dashboard_dealers_mapping',
        external_dag_id='data_ingest_salesforce',
        external_task_id='salesforce_accounts_to_dashboard_dealers_mapping',
        execution_date_fn=get_salesforce_dealer_mapping_data_execution_date,
        poke_interval=300,
        timeout=60 * 60 * 1,
        mode='reschedule')


    #####################################
    # Process Tasks
    #####################################
    # Accounts
    process_dids_google_adwords_accounts_task = DidsSparkBashOperator(
        task_id='process_dids_google_adwords_accounts',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_dids_google_adwords_accounts.py',
        queue=queue_name_1)

    # Campaigns
    process_google_adwords_campaigns_task = DidsSparkBashOperator(
        task_id='process_google_adwords_campaigns',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_adwords_campaigns.py',
        queue=queue_name_1)

    # AdGroups
    process_google_adwords_adgroups_task = DidsSparkBashOperator(
        task_id='process_google_adwords_adgroups',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_adwords_adgroups.py',
        queue=queue_name_1)

    # Daily and Monthly raw stats
    ingest_prizm_google_ads_customer_metrics_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_customer_metrics_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_customer_metrics_dailies_raw.py',
        queue=queue_name_1)

    ingest_prizm_google_ads_customer_conversions_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_customer_conversions_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_customer_conversions_dailies_raw.py',
        queue=queue_name_1)

    ingest_prizm_google_ads_customer_impression_shares_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_customer_impression_shares_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_customer_impression_shares_dailies_raw.py',
        queue=queue_name_1)

    google_adwords_account_monthlies_raw_task = DidsSparkBashOperator(
        task_id='google_adwords_account_monthlies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_account_monthlies_raw.py',
        queue=queue_name_1)

    ingest_prizm_google_ads_campaign_metrics_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_campaign_metrics_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_campaign_metrics_dailies_raw.py',
        queue=queue_name_1)

    ingest_prizm_google_ads_campaign_conversions_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_campaign_conversions_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_campaign_conversions_dailies_raw.py',
        queue=queue_name_1)

    ingest_prizm_google_ads_campaign_impression_shares_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_campaign_impression_shares_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_campaign_impression_shares_dailies_raw.py',
        queue=queue_name_1)

    ingest_prizm_google_ads_campaign_phone_call_metrics_dailies_raw_task = DidsSparkBashOperator(
        task_id='ingest_prizm_google_ads_campaign_phone_call_metrics_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_prizm_google_ads_campaign_phone_call_metrics_dailies_raw.py',
        queue=queue_name_1)

    google_adwords_campaign_monthlies_raw_task = DidsSparkBashOperator(
        task_id='google_adwords_campaign_monthlies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_campaign_monthlies_raw.py',
        queue=queue_name_1)

    google_adwords_adgroup_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_adwords_adgroup_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_adgroup_dailies_raw.py',
        queue=queue_name_1)

    google_adwords_adgroup_conversions_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_adwords_adgroup_conversions_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_adgroup_conversions_dailies_raw.py',
        queue=queue_name_1)

    google_adwords_adgroup_impression_shares_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_adwords_adgroup_impression_shares_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_adgroup_impression_shares_dailies_raw.py',
        queue=queue_name_1)

    # Daily and Monthly stats
    google_adwords_adgroup_dailies_prizm_task = DidsSparkBashOperator(
        task_id='google_adwords_adgroup_dailies_prizm',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_adgroup_dailies_prizm.py',
        queue=queue_name_1)

    process_prizm_google_ads_campaign_dailies_task = DidsSparkBashOperator(
        task_id='process_prizm_google_ads_campaign_dailies',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_prizm_google_ads_campaign_dailies.py',
        queue=queue_name_1)

    google_adwords_campaign_monthlies_prizm_task = DidsSparkBashOperator(
        task_id='google_adwords_campaign_monthlies_prizm',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_campaign_monthlies_prizm.py',
        queue=queue_name_1)

    process_prizm_google_ads_account_dailies_task = DidsSparkBashOperator(
        task_id='process_prizm_google_ads_account_dailies',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_prizm_google_ads_account_dailies.py',
        queue=queue_name_1)

    google_adwords_account_monthlies_prizm_task = DidsSparkBashOperator(
        task_id='google_adwords_account_monthlies_prizm',
        bash_command=f'{default_spark_submit_command_cluster1} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/google_adwords_account_monthlies_prizm.py',
        queue=queue_name_1)

    #####################################
    # ingest jobs
    #####################################
    google_ads_account_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_account_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_ads_account_dailies_raw.py',
        queue=queue_name_2)

    google_ads_campaign_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_campaign_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_ads_campaign_dailies_raw.py',
        queue=queue_name_2)

    google_ads_adgroup_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_adgroup_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_ads_adgroup_dailies_raw.py',
        queue=queue_name_2)

    google_ads_asset_group_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_asset_group_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_asset_group_product_raw.py',
        queue=queue_name_2)

    google_ads_campaign_location_target_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_campaign_location_target_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_ads_campaign_location_target_dailies_raw.py',
        queue=queue_name_2)

    google_ads_keywords_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_keywords_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_ads_keywords_dailies_raw.py',
        queue=queue_name_2)

    google_ads_ads_dailies_raw_task = DidsSparkBashOperator(
        task_id='google_ads_ads_dailies_raw',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/ingest_google_ads_ads_dailies_raw.py',
        queue=queue_name_2)

    # Process jobs
    process_google_ads_campaign_dailies_fuel_task = DidsSparkBashOperator(
        task_id='google_ads_campaign_dailies_fuel',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_ads_campaign_dailies_fuel.py',
        queue=queue_name_2)

    process_google_ads_account_dailies_fuel_task = DidsSparkBashOperator(
        task_id='process_google_ads_account_dailies_fuel',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_ads_account_dailies_fuel.py',
        queue=queue_name_2)

    process_google_ads_adgroup_dailies_fuel_task = DidsSparkBashOperator(
        task_id='google_ads_adgroup_dailies_fuel',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_ads_adgroup_dailies_fuel.py',
        queue=queue_name_2)

    process_google_ads_asset_group_dailies_fuel_task = DidsSparkBashOperator(
        task_id='google_ads_asset_group_dailies_fuel',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_ads_asset_group_dailies.py',
        queue=queue_name_2)

    process_google_ads_ads_dailies_fuel_task = DidsSparkBashOperator(
        task_id='google_ads_ads_dailies_fuel',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_ads_ads_dailies_fuel.py',
        queue=queue_name_2)

    process_google_ads_keywords_dailies_fuel_task = DidsSparkBashOperator(
        task_id='google_ads_keywords_dailies_fuel',
        bash_command=f'{default_spark_submit_command_cluster2} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/process_google_ads_keywords_dailies_fuel.py',
        queue=queue_name_2)

    # # Publish jobs
    publish_google_ads_accounts_task = DidsSparkBashOperator(
        task_id='publish_google_ads_accounts',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_accounts.py',
        queue=queue_name_1)

    publish_dealer_google_ads_accounts_task = DidsSparkBashOperator(
        task_id='publish_dealer_google_ads_accounts',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_dealer_google_ads_accounts.py',
        queue=queue_name_1)

    publish_google_ads_campaigns_task = DidsSparkBashOperator(
        task_id='publish_google_ads_campaigns',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_campaigns.py',
        queue=queue_name_1)

    publish_prizm_google_ads_account_dailies_task = DidsSparkBashOperator(
        task_id='publish_prizm_google_ads_account_dailies',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_prizm_google_ads_account_dailies.py',
        queue=queue_name_1)

    publish_prizm_google_ads_campaign_dailies_task = DidsSparkBashOperator(
        task_id='publish_prizm_google_ads_campaign_dailies',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_prizm_google_ads_campaign_dailies.py',
        queue=queue_name_1)

    publish_google_ads_account_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_account_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_account_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_account_conversions_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_account_conversions_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_account_conversions_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_campaign_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_campaign_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_campaign_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_campaign_conversions_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_campaign_conversions_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_campaign_conversions_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_adgroup_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_adgroup_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_adgroup_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_asset_group_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_asset_group_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_asset_group_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ad_group_performance_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ad_group_performance_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ad_group_performance_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_adgroup_conversions_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_adgroup_conversions_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_adgroup_conversions_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_campaign_location_target_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_campaign_location_target_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_campaign_location_target_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_campaign_location_target_conversions_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_campaign_location_target_conversions_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_campaign_location_target_conversions_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_ads_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_ads_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_ads_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_ads_conversions_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_ads_conversions_dailies_fuel',
        bash_command=f'{publish_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_ads_conversions_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_keywords_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_keywords_dailies_fuel',
        bash_command=f'{publish_keywords_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_keyword_dailies_fuel.py',
        queue=queue_name_2)

    publish_google_ads_keywords_conversions_dailies_fuel_task = DidsSparkBashOperator(
        task_id='publish_google_ads_keywords_conversions_dailies_fuel',
        bash_command=f'{publish_keywords_job_spark_submit_command} {dsc.CODE_ROOT}/di-data_ingest_google_adwords/publish_google_ads_keyword_conversions_dailies_fuel.py',
        queue=queue_name_2)

    #####################################
    # Define the workflow
    #####################################

    # This DAG depends on mapping datasets from other DAGs
    [wait_for_dealers_sensor, wait_for_fuel_clients_to_rox_sites_sensor, wait_for_salesforce_accounts_to_dashboard_dealers_mapping_sensor] >> \
        create_cluster_1_task >> process_dids_google_adwords_accounts_task

    process_dids_google_adwords_accounts_task >> [process_google_adwords_campaigns_task, process_google_adwords_adgroups_task,
                                                  ingest_prizm_google_ads_customer_metrics_dailies_raw_task,
                                                  ingest_prizm_google_ads_customer_conversions_dailies_raw_task,
                                                  ingest_prizm_google_ads_customer_impression_shares_dailies_raw_task,
                                                  google_adwords_account_monthlies_raw_task,
                                                  publish_google_ads_accounts_task,
                                                  publish_dealer_google_ads_accounts_task]

    process_google_adwords_campaigns_task >> [ingest_prizm_google_ads_campaign_metrics_dailies_raw_task,
                                              ingest_prizm_google_ads_campaign_conversions_dailies_raw_task,
                                              ingest_prizm_google_ads_campaign_impression_shares_dailies_raw_task,
                                              ingest_prizm_google_ads_campaign_phone_call_metrics_dailies_raw_task,
                                              google_adwords_campaign_monthlies_raw_task,
                                              publish_google_ads_campaigns_task]


    process_google_adwords_adgroups_task >> [google_adwords_adgroup_dailies_raw_task,
                                             google_adwords_adgroup_conversions_dailies_raw_task,
                                             google_adwords_adgroup_impression_shares_dailies_raw_task
                                             ] >> google_adwords_adgroup_dailies_prizm_task >> kill_cluster_1_task

    [ingest_prizm_google_ads_customer_metrics_dailies_raw_task,
     ingest_prizm_google_ads_campaign_phone_call_metrics_dailies_raw_task,
     ingest_prizm_google_ads_customer_impression_shares_dailies_raw_task] >> process_prizm_google_ads_account_dailies_task >> \
    publish_prizm_google_ads_account_dailies_task >> kill_cluster_1_task

    google_adwords_account_monthlies_raw_task >> google_adwords_account_monthlies_prizm_task >> kill_cluster_1_task

    [ingest_prizm_google_ads_campaign_metrics_dailies_raw_task,
     ingest_prizm_google_ads_campaign_impression_shares_dailies_raw_task,
     ingest_prizm_google_ads_campaign_phone_call_metrics_dailies_raw_task, ] >> process_prizm_google_ads_campaign_dailies_task >> \
    publish_prizm_google_ads_campaign_dailies_task >> kill_cluster_1_task

    google_adwords_campaign_monthlies_raw_task >> google_adwords_campaign_monthlies_prizm_task >> kill_cluster_1_task

    [ingest_prizm_google_ads_campaign_conversions_dailies_raw_task,
     ingest_prizm_google_ads_customer_conversions_dailies_raw_task,
     publish_dealer_google_ads_accounts_task,
     publish_google_ads_accounts_task, publish_google_ads_campaigns_task] >> kill_cluster_1_task

    # Cluster 2 tasks depend on accounts, campaigns, and adgroups being ready
    [process_dids_google_adwords_accounts_task, process_google_adwords_campaigns_task, process_google_adwords_adgroups_task] >> create_cluster_2_task

    create_cluster_2_task >> [google_ads_account_dailies_raw_task, google_ads_campaign_dailies_raw_task,
                              google_ads_adgroup_dailies_raw_task, google_ads_asset_group_dailies_raw_task, google_ads_ads_dailies_raw_task,
                              google_ads_keywords_dailies_raw_task,
                              google_ads_campaign_location_target_dailies_raw_task]

    google_ads_account_dailies_raw_task >> process_google_ads_account_dailies_fuel_task >> [publish_google_ads_account_dailies_fuel_task, publish_google_ads_account_conversions_dailies_fuel_task] >> kill_cluster_2_task

    google_ads_campaign_dailies_raw_task >> process_google_ads_campaign_dailies_fuel_task >> [publish_google_ads_campaign_dailies_fuel_task, publish_google_ads_campaign_conversions_dailies_fuel_task] >> kill_cluster_2_task

    google_ads_adgroup_dailies_raw_task >> process_google_ads_adgroup_dailies_fuel_task >> [publish_google_ads_adgroup_dailies_fuel_task, publish_google_ad_group_performance_dailies_fuel_task, publish_google_ads_adgroup_conversions_dailies_fuel_task] >> kill_cluster_2_task

    google_ads_asset_group_dailies_raw_task >> process_google_ads_asset_group_dailies_fuel_task >> publish_google_ads_asset_group_dailies_fuel_task

    publish_google_ads_adgroup_dailies_fuel_task >> publish_google_ads_asset_group_dailies_fuel_task

    publish_google_ads_adgroup_conversions_dailies_fuel_task >> publish_google_ads_asset_group_dailies_fuel_task

    publish_google_ads_asset_group_dailies_fuel_task >> kill_cluster_2_task

    google_ads_ads_dailies_raw_task >> process_google_ads_ads_dailies_fuel_task >> [publish_google_ads_ads_dailies_fuel_task, publish_google_ads_ads_conversions_dailies_fuel_task] >> kill_cluster_2_task

    google_ads_campaign_location_target_dailies_raw_task >> [publish_google_ads_campaign_location_target_dailies_fuel_task, publish_google_ads_campaign_location_target_conversions_dailies_fuel_task] >> kill_cluster_2_task

    google_ads_keywords_dailies_raw_task >> process_google_ads_keywords_dailies_fuel_task >> [publish_google_ads_keywords_dailies_fuel_task, publish_google_ads_keywords_conversions_dailies_fuel_task] >> kill_cluster_2_task
