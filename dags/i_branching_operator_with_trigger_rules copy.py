from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
# There's also the SQLBranchOperator, between others; not only the PythonBranchOperator
# We'll iterate over these json records to create the dynamic tasks
partners_iterable = {
    "partner_snowflake": {"name" : "snowflake","path" : "/partners/snowflake"},
    "partner_netflix": {"name" : "netflix", "path" : "/partners/netflix"},
    "partner_airflow": {"name" : "airflow", "path" : "/partners/airflow"},
}

def _choosing_partner_based_on_day(execution_date):  
    # returns 1 or multiple task_ids of what task should be executed to PythonBranchOperator
    day = execution_date.day_of_week  # execution date usually is TODAY - 1 (run of previous day)
    if (day == 1):
        return ["extract_partner_snowflake" , "extract_partner_netflix"]
    elif (day == 3):
        return "extract_partner_netflix"
    elif (day == 4):
        return "extract_partner_astronomer"
    else:
        return "stop_task"



# Task Group Tasks will be used after the Extract Dynamic Task in "h_dynamic_tasks"
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


# DAG Definition ahead - Flow is: Start >Extract > Process 
@dag(schedule_interval='@daily', start_date=datetime(2022,1,1), catchup=False, max_active_runs = 1)
def i_branching_operator():

    start = DummyOperator(task_id = "start")

    choose_partner_based_on_day = BranchPythonOperator(
        task_id = "choose_partner_based_on_day",
        python_callable = _choosing_partner_based_on_day  # handles the branching conditions
    )

    stop_task = DummyOperator(task_id="stop_task")
    storing = DummyOperator(task_id="storing_task", trigger_rule="none_failed_or_skipped")
    
    # Dynamic Task based in following for loop
    for i, details in partners_iterable.items():
        
        # Dynamic Tasks
        @task.python(task_id=f"extract_{i}", do_xcom_push=False, multiple_outputs=True)
        def extract(partner_name, partner_path):
            return {"partner_name":partner_name, "partner_path":partner_path}
            
        # DAG Scheduling
        extracted_values = extract(details['name'], details['path']) # handling xcoms
        # if day is not 1, 3, or 5
        choose_partner_based_on_day >> stop_task 
        # when day is 1, 3, or 5
        start >> choose_partner_based_on_day >> extracted_values  # schedule regular tasks
        process_tasks(extracted_values) >> storing  # schedule task group
        
        

# Calling our DAG:
dag = i_branching_operator()