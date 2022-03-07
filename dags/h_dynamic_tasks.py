# taskgroup.py
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging


# We'll iterate over these json records to create the dynamic tasks
partners_iterable = {
    "partner_snowflake":
    {
        "name" : "snowflake",
        "path" : "/partners/snowflake"
    },
    "partner_netflix":
    {
        "name" : "netflix",
        "path" : "/partners/netflix"
    },
    "partner_airflow":
    {
        "name" : "airflow",
        "path" : "/partners/airflow"
    },
}





# DAG Definition
@dag(schedule_interval='@daily', start_date=datetime(2022,1,1), catchup=False, max_active_runs = 1)
def h_dynamic_tasks():

    # Task Group Tasks
    @task.python
    def process_name(partner_name):
        logging.info(partner_name)
        return "success"
    @task.python
    def process_path(partner_path):
        logging.info(partner_path)
        return "success"
    # Task Group itself
    def process_tasks(partner_settings):
        with TaskGroup(group_id="process_tasks", add_suffix_on_collision=True) as process_tasks: 
            # add suffix in collision fix issue with TaskGroup having single group_id
            process_name(partner_settings['partner_name'])
            process_path(partner_settings['partner_path'])
        return process_tasks

    start = DummyOperator(task_id = "start")
    # Dynamic Task based in this for loop
    for i, details in partners_iterable.items():
        # Tasks
        @task.python(task_id=f"extract_{i}", do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name":partner_name, "partner_path":partner_path}
            
        
        # DAG Scheduling
        extracted_values = extract(details['name'], details['path'])
        start >> extracted_values
        process_tasks(extracted_values)


dag = h_dynamic_tasks()