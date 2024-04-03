from airflow.decorators import task, dag
from airflow.providers.docker.operators.docker import DockerOperator

from datetime import datetime

@dag(start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False)
def docker_dag():
    @task()
    def t1():
        pass

    t2 = DockerOperator(
        task_id='t2',
        image='python:3.8-slim-buster',
        command='echo "command running in the docker container"',
        # connect to remote docker rest api
        docker_url='tcp://192.168.1.28:2375',
        network_mode='host',
        container_name='docker_dag_t2',
        api_version='auto',
        auto_remove=True,
        tty=True,
        xcom_all=False,
        mount_tmp_dir=False
    )
    t1() >> t2

dag = docker_dag()
