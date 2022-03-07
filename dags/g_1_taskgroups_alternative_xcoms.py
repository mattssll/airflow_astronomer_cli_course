# taskgroup.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import task, dag
from airflow.utils.task_group import TaskGroup
from datetime import datetime

from groups.g_2_process_tasks import process_tasks


@task.python(task_id="extract_partners", do_xcom_push=False, multiple_outputs=True)
def extract():
    partner_name = "netflix"
    partner_path = '/partners/netflix'
    return {"partner_name":partner_name, "partner_path":partner_path}


@dag(schedule_interval='@daily', start_date=datetime(2022,1,1), catchup=False, max_active_runs = 1)
def g_1_taskgroups_alternative_xcoms():

    partner_settings = extract()

    process_tasks(partner_settings)

dag = g_1_taskgroups_alternative_xcoms()