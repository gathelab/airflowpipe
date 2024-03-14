"""
test.py
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.sensors.external_task_sensor import ExternalTaskSensor
from datetime import datetime, timedelta
from InfraUtils import *
from airflow.operators.dagrun_operator import TriggerDagRunOperator

default_args = {
    'owner':               'ubuntu',
    'depends_on_past':     False,
    'start_date':          datetime(2018, 3, 1),
    'email':               ['airflow@example.com'],
    'email_on_failure':    False,
    'email_on_retry':      False,
    # 'retries':             1,
    # 'retry_delay':         timedelta(seconds=5),
    # 'on_failure_callback': task_fail_slack_alert,
    # 'on_success_callback': task_fail_slack_alert,
    # 'execution_timeout':   timedelta(seconds=10),
    # 'sla':                 timedelta(seconds=10),  # Note, can't have SLA active on None schedule
}

# Initialize the DAG
schedule_interval = None
# schedule_interval = '*/30 * * * *'
with DAG(dag_id='test2', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=sla_missed_slack_alert) as dag:
    # dag.doc_md = "Is this a test?"
    queue_name = 'Dev3'
    # sleep_task = BashOperator(
    #     task_id='sleep_task',
    #     bash_command='sleep 600',
    #     queue='meow'
    # )
    # test1_sensor = ExternalTaskSensor(
    #     task_id='test1_sensor',
    #     external_dag_id='test',
    #     external_task_id='echo_task',
    #     poke_interval=10, # 300
    #     timeout=60*60,
    #     mode='reschedule',
    # )

    many_tasks = [
        BashOperator(
            task_id=f'echo_task_{i}',
            bash_command='sleep 1; exit 1;',
            queue=queue_name
        ) for i in range(0,10)
    ]
    # echo_task = BashOperator(
    #     task_id='echo_task',
    #     bash_command='echo "meow"',
    #     queue=queue_name
    # )
    # echo_task.doc_md = 'This is a test'

    # echo_task2 = BashOperator(
    #     task_id='echo_task2',
    #     bash_command='echo "meow"',
    #     queue='hyder1')

    # trigger = TriggerDagRunOperator(
    #     task_id='trigger_roxanne_references',
    #     trigger_dag_id="roxanne_references",
    #     # python_callable=conditionally_trigger,
    #     # params={'condition_param': True, 'message': 'Hello World'},
    #     # dag=dag,
    # )
    # Define the workflow
    # echo_task >> echo_task2 #>> trigger
