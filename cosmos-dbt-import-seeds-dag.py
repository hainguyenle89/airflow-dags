from airflow import DAG
from airflow.datasets import Dataset
from airflow.utils.task_group import TaskGroup
from pendulum import datetime
#import os
#from datetime import datetime
#from pathlib import Path

from cosmos import ProfileConfig
from cosmos.operators import DbtRunOperationOperator, DbtSeedOperator
from cosmos.profiles import PostgresUserPasswordProfileMapping

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
    dag_id="cosmos-dbt-import-seeds",
    start_date=datetime(2023, 1, 1),
    schedule=None,
    #schedule_interval="@daily",
    doc_md=__doc__,
    catchup=False,
    max_active_runs=1,
#    default_args={"owner": "01-EXTRACT", "retries": 2},
) as dag:

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
        #dbt_executable_path="/opt/bitnami/airflow/dbt_venv/bin/dbt",
        profile_config=profile_config,
        outlets=[Dataset(f"SEED://JAFFLE_SHOP")],
        install_deps=True,
     )

    drop_seeds >> create_seeds
