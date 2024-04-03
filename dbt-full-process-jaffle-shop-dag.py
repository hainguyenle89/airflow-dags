from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.operators import DbtRunOperationOperator, DbtSeedOperator
from airflow.utils.task_group import TaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from pendulum import datetime
from airflow import DAG
from cosmos.operators import (
    DbtDocsOperator,
    DbtDocsS3Operator
)
from cosmos.profiles import PostgresUserPasswordProfileMapping
from cosmos.log import get_logger
from airflow.exceptions import AirflowNotFoundException

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
    dag_id="Dbt_full_process_jaffle_shop_dag",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    #default_args={"retries": 2},
    catchup=False,
    doc_md=__doc__,
    max_active_runs=1,
):
    project_seeds = [
        {
            "project": "jaffle_shop",
            "seeds": ["raw_customers", "raw_payments", "raw_orders"],
        }
    ]

    with TaskGroup(group_id="drop_seeds_if_exist") as drop_seeds:
        for project in project_seeds:
            for seed in project["seeds"]:
                DbtRunOperationOperator(
                    task_id=f"drop_{seed}_if_exists",
                    macro_name="drop_table",
                    args={"table_name": seed},
                    #project_dir=DBT_ROOT_PATH / "jaffle_shop",
                    project_dir=f"/opt/bitnami/airflow/dags/dbt/{project['project']}",
                    #dbt_executable_path="/opt/bitnami/airflow/dbt_venv/bin/dbt",
                    profile_config=profile_config,
                    install_deps=True,
                )

    create_seeds = DbtSeedOperator(
        task_id=f"jaffle_shop_seed",
        project_dir=f"/opt/bitnami/airflow/dags/dbt/jaffle_shop",
        #use the default python virtual environment in /opt/bitnami/airflow/venv
        #dbt_executable_path="/opt/bitnami/airflow/dbt_venv/bin/dbt",
        profile_config=profile_config,
        install_deps=True,
     )

    jaffle_shop_tg = DbtTaskGroup(
        group_id="jaffle_shop_tg",
        project_config=ProjectConfig(
            #DBT_ROOT_PATH / "jaffle_shop",
            "/opt/bitnami/airflow/dags/dbt/jaffle_shop",
        ),
        #dbt_project_name="jaffle_shop",
        profile_config=profile_config,
        operator_args={
            "install_deps": True,  # install any necessary dependencies before running any dbt command
            # astronomer-cosmos==1.0.4 and dbt-core==1.5.0 do not support the flag "full_refresh"
            #"full_refresh": True,  # used only in dbt commands that support this flag
        },
        execution_config=ExecutionConfig(
            #dbt_executable_path="/opt/bitnami/airflow/dbt_venv/bin/dbt",
            dbt_executable_path="/opt/bitnami/airflow/venv/bin/dbt",
        ),
    )

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

    drop_seeds >> create_seeds >> jaffle_shop_tg >> [generate_dbt_docs_ozone_awsS3,generate_dbt_docs_local]
