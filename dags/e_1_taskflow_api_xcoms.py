from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.decorators import dag, task


@task.python(task_id="extract_xcoms", multiple_outputs=True)
def extract(): # will be name of task
    # Multiple outputs will send value_1 and value_2 as two different xcoms instead of 1 xcom dict
    my_xcom_value_1 = "hello"
    my_xcom_value_2 = "world"
    return {"value_1": my_xcom_value_1, "value_2" : my_xcom_value_2}  # this will push 

@task.python
def process(my_xcoms):
    print(f"printing xcoms: {my_xcoms['value_1']}, {my_xcoms['value_2']}")
        

@dag(schedule_interval='@daily', start_date=datetime(2021, 12, 1), catchup=False)
def e_taskflow_xcoms_1():

    process(extract())

dag = f_taskflow_xcoms_1()