from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.decorators import task, dag
from datetime import datetime
import logging 

# Define Tasks
@task.python(task_id = "logging_stuff")
def logging_stuff_task():

    logging.info('Hello')
    logging.debug('This is a debug message')
    logging.info('This is an info message')
    logging.warning('This is a warning message')
    logging.error('This is an error message')
    logging.critical('This is a critical message')


# Define DAG
@dag(schedule_interval='@daily', start_date=datetime(2021, 12, 1), catchup=False)
def a_1_logging_basics():

    logging_stuff_task()


# Schedule Pipeline
dag = a_1_logging_basics()
    