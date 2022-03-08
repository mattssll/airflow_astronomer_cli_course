from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.task_group import TaskGroup
from datetime import datetime
from airflow.models.baseoperator import cross_downstream, chain # used for dependencies


# DAG Definition ahead - Flow is: Start >Extract > Process 
@dag(schedule_interval='@daily', start_date=datetime(2022,1,1), catchup=False, max_active_runs = 1)
def k_task_dependencies():

    t1 = DummyOperator(task_id = "t1")
    t2 = DummyOperator(task_id = "t2")
    t3 = DummyOperator(task_id = "t3")
    t4 = DummyOperator(task_id = "t4")
    t5 = DummyOperator(task_id = "t5")
    t6 = DummyOperator(task_id = "t6")
    t7 = DummyOperator(task_id = "t7")

    # Old Way:
    #t1.set_downstream(t2)
    #t2.set_upstream(t1)
    # "New" Way:
    #t1 >> t2 >> t3
    #t2 << t2 << t1
    #[t1, t2, t3] >> t5 # lists on both ends of >> won't work
    # unless the following is used (CROSS_DOWNSTREAM):
    #cross_downstream([t1,t2,t3], [t4,t5,t6]) # single items from 2nd list depends on all items from first list
    #[t4, t5, t6] >> t7
    # now let's take a look at CHAIN:
    #chain(t1, [t2,t3], [t4, t5]) # both lists have to be same size
    # Mix cross_downstream and chain:
    cross_downstream([t2,t3], [t4,t5])
    chain(t1, t2, t5, t6)
# Calling our DAG:
dag = k_task_dependencies()