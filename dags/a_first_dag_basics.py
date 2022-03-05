from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

class CustomPostgresOperator(PostgresOperator):
    template_fields = ('sql', 'parameters')

# Dag is executed on start_date + schedule interval

def _extract_var(partner_name):
    print(f"partner name is : {partner_name}")
    

with DAG(dag_id="a_first_dag_basics",
         description="first video from astronomer",
         start_date=datetime(2022,3,1),
         schedule_interval="1 12 * * *",  # timedelta(minutes=5)), @daily, @weekly; also available
         dagrun_timeout = timedelta(minutes=30),
         tags = ["video lesson 1", "data engineering"],
         catchup = False) as dag:  # 3rd of march, 2022
         # if start date is from past, False will NOT generate multiple runs
           
         extract = PythonOperator(
             task_id="get_airflow_variable",
             python_callable = _extract_var,
             op_args=["{{ var.json.my_dag_partner.name }}"]  # jinja templating engine avoids connection being made to db all the time
         )

         fetch_data = CustomPostgresOperator(
             task_id="fetching_data",
             sql="sql/my_query.sql",
             parameters={
                 'next_ds': '{{ next_ds }}',
                 'prev_ds': '{{ prev_ds }}'
            }
         )


         extract >> fetch_data