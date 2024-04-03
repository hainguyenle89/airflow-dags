from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig
from cosmos.operators import (
    DbtDocsOperator,
    DbtDocsS3Operator
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.log import get_logger
from airflow.datasets import Dataset
from pendulum import datetime
from airflow import DAG
from airflow.hooks.base import BaseHook
from airflow.exceptions import AirflowNotFoundException
from airflow.decorators import task
from airflow.operators.bash import BashOperator

import os,shutil
#from pathlib import Path

#DEFAULT_DBT_ROOT_PATH = Path(__file__).parent / "dbt"
#DBT_ROOT_PATH = Path(os.getenv("DBT_ROOT_PATH", DEFAULT_DBT_ROOT_PATH))

profile_config = ProfileConfig(
    profile_name="jaffle_shop",
    target_name="dev",
    profile_mapping=PostgresUserPasswordProfileMapping(
        conn_id="postgres_thinklabs_dev_dbt",
        profile_args={"schema": "jaffe_shop"},
    ),
)

logger = get_logger(__name__)

#S3_CONN_ID = "aws_hainguyenle89"
S3_CONN_ID = "ozone_aws_s3_local"

# test ping to aws endpoint url
#response = os.popen('ping -n 5 bigdata2.bigdatavnpt.com')
#for line in response.readlines():
#    print(line)


# https://stackoverflow.com/questions/1868714/how-do-i-copy-an-entire-directory-of-files-into-an-existing-directory-using-pyth
def copytree(src, dst, symlinks=False, ignore=None):
    for item in os.listdir(src):
        s = os.path.join(src, item)
        d = os.path.join(dst, item)
        if os.path.isdir(s):
            shutil.copytree(s, d, symlinks, ignore)
        else:
            shutil.copy2(s, d)

def copy_docs_to_local_dir(project_dir: str):
    target_dir = f"{project_dir}/target"
    destination_dir='/opt/bitnami/airflow/dags/dbt/jaffle_shop/target'
    isExist = os.path.exists(destination_dir)
    if not isExist:
        os.makedirs(destination_dir)
        logger.info("Destination folder '%s' is created!",destination_dir)
    else:
        shutil.rmtree(destination_dir)
        os.makedirs(destination_dir)
    logger.info("Attempting to copy generated docs from '%s' to local storage '%s'...",target_dir,destination_dir)
    copytree(target_dir,destination_dir)


with DAG(
    dag_id="cosmos_dbt_docs_jaffle_shop_dag",
    start_date=datetime(2023, 1, 1),
    #schedule_interval="@daily",
    schedule=None,
    doc_md=__doc__,
    catchup=False,
    #default_args={"retries": 2},
) as dag:
    generate_dbt_docs_ozone_awsS3 = DbtDocsS3Operator(
        task_id="generate_dbt_docs_ozone_awsS3",
        project_dir=f"/opt/bitnami/airflow/dags/dbt/jaffle_shop",
        #dbt_executable_path="/opt/bitnami/airflow/dbt_venv/bin/dbt",
        dbt_executable_path="/opt/bitnami/airflow/venv/bin/dbt",
        install_deps=True,
        profile_config=profile_config,
        #aws_conn_id=S3_CONN_ID,  -> will be deprecated in cosmos 2.0
        connection_id=S3_CONN_ID,
        bucket_name="airflowdbt",
        folder_dir="jaffle_shop_dbt_docs",
    )

    generate_dbt_docs_local = DbtDocsOperator(
        task_id="generate_dbt_docs_local",
        project_dir=f"/opt/bitnami/airflow/dags/dbt/jaffle_shop",
        #dbt_executable_path="/opt/bitnami/airflow/dbt_venv/bin/dbt",
        dbt_executable_path="/opt/bitnami/airflow/venv/bin/dbt",
        profile_config=profile_config,
        install_deps=True,
        callback=copy_docs_to_local_dir,
    )

    ## can not run a command in background by BashOperator
    #serve_dbt_docs_http_14500 = BashOperator(
    #    task_id="serve_dbt_docs_http_14500",
    #    bash_command="cd /opt/bitnami/airflow/dags/dbt/jaffle_shop && \
    #                  nohup /opt/bitnami/airflow/venv/bin/dbt docs serve --port 14500 --profiles-dir ./ --target dev",
    #    #env={"EXACT": "yes", "NUMBER": "10"},
    #    #append_env=True,
    #) 

    #[generate_dbt_docs_local,generate_dbt_docs_ozone_awsS3] >> serve_dbt_docs_http_14500
    generate_dbt_docs_local >> generate_dbt_docs_ozone_awsS3

