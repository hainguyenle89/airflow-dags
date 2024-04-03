from airflow.decorators import task, dag
#from airflow.providers.docker.operators.docker import DockerOperator
from airflow.providers.docker.operators.docker_swarm import DockerSwarmOperator

from datetime import datetime

@dag(start_date=datetime(2022, 1, 1), schedule_interval='@daily', catchup=False)
def dockerswarm_dag():
    @task()
    def t1():
        pass

    t2 = DockerSwarmOperator(
        task_id='dockerSwarmTask',
        image='python:3.8-slim-buster',
        command='echo "command running in the docker container"',
        # connect to remote docker rest api
        docker_url='tcp://192.168.1.28:2375',
        api_version='auto',
        tty=True,
        auto_remove=True,
    )
    t1() >> t2

dag = dockerswarm_dag()
