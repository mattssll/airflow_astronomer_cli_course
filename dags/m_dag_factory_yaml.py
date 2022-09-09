from airflow import DAG
import dagfactory

docker_path = '/usr/local/airflow'
dag_folder_path = '/dags/yaml_dags'

dag_factory = dagfactory.DagFactory(f"{docker_path}{dag_folder_path}/dag_1.yml")

dag_factory.clean_dags(globals())
dag_factory.generate_dags(globals())
