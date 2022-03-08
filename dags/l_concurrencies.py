from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.models.baseoperator import cross_downstream, chain # used for dependencies

# paralellism (airflow level) = 32, Amount of tasks whole airflow can run at the same time, when specified for single dag
# concurrency (dag level) = 16, At most 16 tasks for all dag runs of a given DAG 
# max_active_runs_per_dag (dag level) = 16, At most 16 DAG runs at the same time of a specific DAG
# task_concurrency (task level) = Max active runs in multiple runs of same dag for TASK

# DAG Definition ahead - Flow is: Start >Extract > Process 
@dag(schedule_interval='@daily', start_date=datetime(2022,1,1), catchup=False,
    concurrency=2, max_active_runs = 1)
def l_concurrencies():

    t1 = DummyOperator(task_id = "t1", task_concurrency = 1)
    t2 = DummyOperator(task_id = "t2")
    t3 = DummyOperator(task_id = "t3")

    [t1, t2] >> t3
# Calling our DAG:
dag = l_concurrencies()