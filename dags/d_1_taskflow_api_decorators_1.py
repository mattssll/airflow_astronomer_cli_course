from airflow.decorators import dag, task
from datetime import timedelta, datetime

# 1. Tasks - Decorator must have a function below it, below are our 2 tasks (set as airflow operator before, now as)

    
# 2. Defining our DAG
@dag(start_date=datetime(2022,3,1), schedule_interval="1 12 * * *", catchup = False)
def d_taskflow_api():
    @task.python
    def extract():
        my_xcom_value_1 = "hello"
        my_xcom_value_2 = "world"
        return my_xcom_value_1
    @task.python
    def process(my_xcom_value_1):
        print(my_xcom_value_1)

    process(extract())  # New way of handling dependencies

# 4. Call our DAG    
dag = d_taskflow_api()