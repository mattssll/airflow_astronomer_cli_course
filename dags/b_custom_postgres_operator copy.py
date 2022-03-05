from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')

    

with DAG(dag_id="b_dag_postgres_operator",
         description="testing the postgres custom operator",
         start_date=datetime(2022,3,1),
         schedule_interval="1 12 * * *",  # timedelta(minutes=5)), @daily, @weekly; also available
         dagrun_timeout = timedelta(minutes=30),
         tags = ["video lesson 2", "data engineering"],
         catchup = False) as dag:  # if start date is from past, False will NOT generate multiple runs
            
    # defining our tasks
    fetch_data = CustomPostgresOperator(
        task_id="fetching_data",
        sql="sql/my_query.sql",
        parameters={
            'next_ds': '{{ next_ds }}',
            'prev_ds': '{{ prev_ds }}'
        }
    )
    # scheduling
    fetch_data