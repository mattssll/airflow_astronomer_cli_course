FROM quay.io/astronomer/ap-airflow:latest
COPY requirements.txt /tmp/
RUN pip install -r /tmp/requirements.txt
ENV MY_ENV_VAR = '{"name" : "jay again", "secret" : "this is my secret"}'