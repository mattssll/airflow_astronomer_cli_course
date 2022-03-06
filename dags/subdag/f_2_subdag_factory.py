"""Helper function to generate a DAG and operators given some arguments."""

# [START subdag]
import pendulum

from airflow import DAG
from airflow.operators.dummy import DummyOperator


def subdag(parent_dag_name, child_dag_name, args):
    """
    Generate a DAG to be used as a subdag.
    :param str parent_dag_name: Id of the parent DAG
    :param str child_dag_name: Id of the child DAG
    :param dict args: Default arguments to provide to the subdag
    :return: DAG to use as a subdag
    :rtype: airflow.models.DAG
    """
    dag_subdag = DAG(
        dag_id=f'{parent_dag_name}.{child_dag_name}',
        default_args=args,
        start_date=pendulum.datetime(2021, 1, 1, tz="UTC"),
        catchup=False,
        schedule_interval="@daily",
    )

    for i in range(5):
        DummyOperator(
            task_id=f'{child_dag_name}-task-{i + 1}',
            default_args=args,
            dag=dag_subdag,
        )

    return dag_subdag


# [END subdag]