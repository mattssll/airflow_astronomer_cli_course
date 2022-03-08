from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task
from typing import Dict


@task.python(task_id="extract_xcoms", multiple_outputs=True)
def extract(task_id = 'extract_xcoms') -> Dict[str, str]: # same as multiple_outputs
    # Multiple outputs will send value_1 and value_2 as two different xcoms instead of 1 xcom dict
    my_xcom_value_1 = "hello"
    my_xcom_value_2 = "world"
    return {"value_1": my_xcom_value_1, "value_2" : my_xcom_value_2, "value_3" : 100}  # this will push 

@task.python
def process(my_xcoms):
    print(f"printing xcoms: {my_xcoms['value_1']}, {my_xcoms['value_2']}, {my_xcoms['value_3']}")
        

@dag(schedule_interval='@daily', start_date=datetime(2021, 12, 1), catchup=False, dagrun_timeout = timedelta(minutes=30))
def e_taskflow_xcoms_2():

    process(extract())

dag = e_taskflow_xcoms_2()