from airflow import DAG
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'haing',
    'start_date': datetime(2024, 5, 20),
    #'retries': 2,
	#  'retry_delay': timedelta(minutes=2)
}

with DAG(
    dag_id="telecom-data-product-test",
    #start_date=datetime(2024, 5, 22),
    default_args=default_args,
    schedule=None,
    #schedule_interval="@daily",
    catchup=False,
    max_active_runs=1,
#    default_args={"owner": "01-EXTRACT", "retries": 2},
) as dag:
    telecom_subscriber_iceberg_streaming_etl = SparkSubmitOperator(
        application="/opt/bitnami/airflow/dags/python_projects/telecom_data_product/telecom_product_application.py",
        #application="/opt/bitnami/airflow/dags/python_projects/telecom_data_product/tests/test_db.py",
        task_id="spark_telecom_subscriber_iceberg_streaming_etl",
        packages="io.delta:delta-spark_2.12:3.2.0,org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.5.2,org.apache.kudu:kudu-spark3_2.12:1.17.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        py_files="/opt/bitnami/airflow/dags/python_projects/telecom_data_product/packages.zip",
        files="/opt/bitnami/airflow/dags/python_projects/telecom_data_product/configs/telecom_etl_config.yaml",
        conn_id="spark_master",
        spark_binary="/opt/bitnami/airflow/spark-3.5.1-bin-hadoop3/bin/spark-submit",
        jars="/opt/bitnami/airflow/dags/python_projects/libraries/postgresql-42.6.0.jar",
        #env_vars={'etlJob': 'subscriberIcebergETL'},
        application_args=['subscriberIcebergStreamingETL'],
    )

    telecom_subscriber_iceberg_streaming_etl

