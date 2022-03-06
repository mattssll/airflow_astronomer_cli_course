from subdag.f_2_subdag_factory import subdag
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.dates import days_ago

DAG_NAME = 'f_1_subdag_main'

with DAG(
    dag_id=DAG_NAME,
    default_args={"retries": 2},
    start_date=days_ago(2),
    schedule_interval="@once",
) as dag:

    start = DummyOperator(
        task_id='start',
    )

    section_1 = SubDagOperator(
        task_id='section-1',
        subdag=subdag(DAG_NAME, 'section-1', dag.default_args),
    )

    some_other_task = DummyOperator(
        task_id='some-other-task',
    )

    section_2 = SubDagOperator(
        task_id='section-2',
        subdag=subdag(DAG_NAME, 'section-2', dag.default_args),
    )

    end = DummyOperator(
        task_id='end',
    )

    start >> section_1 >> some_other_task >> section_2 >> end
# [END example_subdag_operator]

