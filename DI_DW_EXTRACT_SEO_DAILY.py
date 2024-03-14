from airflow import DAG
from airflow.models.baseoperator import chain
from airflow.models.dagrun import DagRun
from airflow.operators.bash import BashOperator
from airflow.operators.email import EmailOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.microsoft.winrm.hooks.winrm import WinRMHook
from airflow.providers.microsoft.winrm.operators.winrm import WinRMOperator
from airflow.sensors.external_task import ExternalTaskSensor
from cars.callbacks import slack_alert, bobby_tables, dw_job_monitors, data_streaming_alerts
from cars.operators.cars_bash_operator import CarsBashOperator
from cars.timetables.cron_list_timetable import CronListTimetable
from cars.timetables.first_monday_of_the_month import FIRST_MONDAY_OF_THE_MONTHTimetable
from cars.timetables.holidays import HOLIDAYSTimetable
from cars.utils import get_last_dagrun
from datetime import datetime


with DAG(
    dag_id="DW_DI_EXTRACT_SEO",
    params={},
    max_active_tasks=10,
    schedule=None,
    start_date=datetime(2022, 1, 1),
    default_args={'on_failure_callback': [slack_alert], 'owner': 'airflow'},
    catchup=False,
    tags=['airflow'],
) as dag:

    start = EmptyOperator(task_id='start',)

    xt_2 = ExternalTaskSensor(
        task_id='xt_CUSTOMER_2',
        external_dag_id='CUSTOMER',
        external_task_group_id=None,
        execution_date_fn=get_last_dagrun,
        allowed_states=['success'],
        check_existence=True,
    )

    xt_3 = ExternalTaskSensor(
        task_id='xt_GOOGLE_SEARCH_CONSOLE_3',
        external_dag_id='data_ingest_google_search_console',
        external_task_group_id=None,
        execution_date_fn=get_last_dagrun,
        allowed_states=['success'],
        check_existence=True,
    )

    xt_4 = ExternalTaskSensor(
        task_id='xt_SEMRUSH_4',
        external_dag_id='data_ingest_semrush',
        external_task_group_id=None,
        execution_date_fn=get_last_dagrun,
        allowed_states=['success'],
        check_existence=True,
    )

    xt_5 = ExternalTaskSensor(
        task_id='xt_GOOGLE_MY_BUSINESS_5',
        external_dag_id='data_ingest_google_my_business',
        external_task_group_id=None,
        execution_date_fn=get_last_dagrun,
        allowed_states=['success'],
        check_existence=True,
    )
    
    
    #
    xt_6 = ExternalTaskSensor(
        task_id='xt_GOOGLE_ANALYTICS4_6',
        external_dag_id='Data_ingest_google_analytics4',
        external_task_group_id=None,
        execution_date_fn=get_last_dagrun,
        allowed_states=['success'],
        check_existence=True,
    )


    hk = WinRMHook(ssh_conn_id='CA2TBW003', transport='ntlm')
    t_7 = WinRMOperator(task_id="t_DW_DI_EXTRACT_SEO_8",
        trigger_rule="all_success",
        command="c:\\Misc\\Prizm\\DI_SEO_Daily.bat",
        winrm_hook=hk,
        sla=4
    )

    repl = {'&system#': 'Airflow', '&client#': '', '&parentname#': '{{ task_instance_key_str }}', '&parentnr#': '{{ run_id }}'}

    t_8 = EmailOperator(
        task_id='DW_JOB_DR_FINISH_9',
        trigger_rule='none_failed',
        to='DWBI-Reports@cars.com',
        subject=f"{repl['&parentname#']}: FINISHED - {repl['&system#']}, client: {repl['&client#']}",
        html_content=f'''The object: {repl['&parentname#']} has finished. 
    
    System: {repl['&system#']}
    Client: {repl['&client#']}
    
    Object: {repl['&parentname#']} 
    Run number: {repl['&parentnr#']}''',
    )

    end = EmptyOperator(task_id='end', trigger_rule='none_failed')

    [xt_2, xt_3, xt_4, xt_5, xt_6, start] >> t_7

    [t_7] >> t_8

    [t_8] >> end
