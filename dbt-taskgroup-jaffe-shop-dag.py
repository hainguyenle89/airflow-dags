from cosmos import ProjectConfig, ProfileConfig, ExecutionConfig, DbtTaskGroup
from cosmos.profiles import PostgresUserPasswordProfileMapping
from airflow.datasets import Dataset
from pendulum import datetime
from airflow.operators.empty import EmptyOperator
from airflow import DAG

#import os
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

with DAG(
    dag_id="cosmos_DbtTaskGroup_jaffle_shop_dag",
    start_date=datetime(2023, 1, 1),
    #schedule=None,
    schedule=[Dataset(f"SEED://JAFFLE_SHOP")],
    #default_args={"retries": 2},
    catchup=False,
):
    #e1 = EmptyOperator(task_id="pre_dbt")

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

    #e2 = EmptyOperator(task_id="post_dbt")
    #e1 >> jaffle_shop_tg >> e2
    jaffle_shop_tg
