from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.decorators import task, dag, task_group
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import logging
"""
all_success: (default) all parents have succeeded
all_failed: all parents are in a failed or upstream_failed state
all_done: all parents are done with their execution
one_failed: fires as soon as at least one parent has failed, it does not wait for all parents to be done
one_success: fires as soon as at least one parent succeeds, it does not wait for all parents to be done
none_failed: all parents have not failed (failed or upstream_failed) i.e. all parents have succeeded or been skipped
none_skipped: no parent is in a skipped state, i.e. all parents are in a success, failed, or upstream_failed state
none_failed_or_skipped: NONE_FAILED_OR_SKIPPED skips the current task only if all upstream tasks are skipped, otherwise continues. - used with BranchPythonOperator
dummy: dependencies are just for show, trigger at will
"""


# DAG Definition ahead - Flow is: Start >Extract > Process 
@dag(schedule_interval='@daily', start_date=datetime(2022,1,1), catchup=False, max_active_runs = 1)
def j_trigger_rules():

    start = DummyOperator(task_id = "start", trigger_rule="all_success") # Default
    middle = DummyOperator(task_id = "middle", trigger_rule="none_failed")
    end = DummyOperator(task_id = "end", trigger_rule="one_failed")

    start >> middle >> end
    

# Calling our DAG:
dag = j_trigger_rules()