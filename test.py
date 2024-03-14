"""
test.py
"""

from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from datetime import datetime, timedelta
from InfraUtils import *
from airflow.operators.dagrun_operator import TriggerDagRunOperator
from airflow.sensors.time_sensor import TimeSensor

default_args = {
    'owner':               'ubuntu',
    'depends_on_past':     False,
    'start_date':          datetime(2018, 3, 1),
    # 'retries':             1,
    # 'retry_delay':         timedelta(seconds=5),
    # 'on_failure_callback': task_fail_slack_alert,
    # 'on_success_callback': task_fail_slack_alert,
    # 'execution_timeout':   timedelta(seconds=10),
    # 'sla':                 timedelta(seconds=10),  # Note, can't have SLA active on None schedule
}

# Initialize the DAG
# schedule_interval = None
schedule_interval = '* * * * *'
with DAG(dag_id='test', default_args=default_args, schedule_interval=schedule_interval,
         sla_miss_callback=sla_missed_slack_alert) as dag:
    # dag.doc_md = "Is this a test?"
    queue_name = 'Dev3'
    # sleep_task = BashOperator(
    #     task_id='sleep_task',
    #     bash_command='sleep 600',
    #     queue='meow'
    # )
    #
    # target_time = datetime.now().replace(hour=17, minute=40, second=0)
    # sensor_task = TimeSensor(
    #     target_time=target_time.time(),
    #     task_id='wait_for_time_task',
    #     poke_interval=10,
    #     timeout=60 * 60 * 12,
    #     mode='reschedule'
    # )
    #

    task_1 = BashOperator(
        task_id='task_1',
        bash_command='exit 0',
    )

    task_2 = BashOperator(
        task_id='task_2',
        bash_command='exit 1',
        depends_on_past=True,
    )


    # echo_task = BashOperator(
    #     task_id='echo_task_1',
    #     bash_command='echo "meow"',
    #     # queue=queue_name
    # )
    # echo_task.doc_md = 'This is a test'
    #
    # echo_task2 = BashOperator(
    #     task_id='echo_task2',
    #     bash_command='echo "meow"',
    #     # queue='hyder1'
    # )
    #
    # [echo_task, sensor_task] >> echo_task2

    # trigger = TriggerDagRunOperator(
    #     task_id='trigger_roxanne_references',
    #     trigger_dag_id="roxanne_references",
    #     # python_callable=conditionally_trigger,
    #     # params={'condition_param': True, 'message': 'Hello World'},
    #     # dag=dag,
    # )
    # Define the workflow
    # echo_task >> echo_task2 #>> trigger
