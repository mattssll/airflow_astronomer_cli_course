# Astronomer Airflow Course
First Make sure you installed the Astronomer CLI - the below is for MAC OS: 
```
brew install astronomer/tap/astro
```

<br>Default passwords are - login: admin, password: admin

## To start a new environment and repo:
```
astro dev init
```
Don't run this after cloning this repo, only run this to start a new repo from scratch.

## To start airflow from Dockerfile after step above:
```
astro dev start
```

## Check containers
```
astro dev ps
```

## To stop the docker images of airflow:
```
astro dev stop
```

## Some airflow CLI Commands
```
# Backfilling
airflow dags backfill -s 2022-01-01 -e 2022-01-30
# Run a task
airflow tasks test my_dag_name task_name 2021-01-01 # date is the "dag run" date
airflow tasks test my_dag_name task_name 2021-01-01
```