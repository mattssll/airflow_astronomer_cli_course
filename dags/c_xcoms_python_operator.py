from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator

# Task instance object (object created when task is triggered), needed to access xcoms
def _extract(ti):
    # 1st classic way of pushing xcoms
    #ti.xcom_push(key="my_xcom_value", value=100)
    # 2nd way will use regular vars and return it
    my_xcom_value_1 = "hello"
    my_xcom_value_2 = "world"
    return {"value_1": my_xcom_value_1, "value_2" : my_xcom_value_2}  # this will push 


def _process(ti):
    # 1st classic way of accessing xcoms
    #my_xcom_value = ti.xcom_pull(key="my_xcom_value", task_ids="extract") # task id where xcom was pushed
    # 2nd way of accessing xcoms
    my_xcom_values_dict = ti.xcom_pull(task_ids="push_airflow_variable")  # key can be hidden or can be used "return_value"
    print(f"printing the incoming xcom value: {my_xcom_values_dict['value_1']}")
    

with DAG(dag_id = "c_xcoms_python_operator",
         description="testing the postgres custom operator",
         start_date=datetime(2022,3,1),
         schedule_interval="1 12 * * *",  # timedelta(minutes=5)), @daily, @weekly; also available
         dagrun_timeout = timedelta(minutes=30),
         tags = ["video lesson 3", "data engineering"],
         catchup = False  # if start date is from past, False will NOT generate multiple runs
         ) as dag: 


    extract = PythonOperator(
             task_id="push_airflow_variable",
             python_callable = _extract
         )

    process = PythonOperator(
             task_id="get_airflow_variable",
             python_callable = _process
         )

    # scheduling
    extract >> process