"""
### Features monitoring DAG.
#### Purpose
DAG takes data and calc some statistics for monitoring in time.
#### Notes
- Check README.md before using
- [Main GitHub repository](https://github.com/slavkostrov/flight_delays_ml_system)
"""
import datetime

from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator as EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator

from base_config import Config
from tasks.prepare_features import collect_raw_data, clean_data, concat_features

config = Config()

with DAG(
    f"{config.dag_prefix}features_monitoring",
    default_args={
        "depends_on_past": False,
        "email": [config.user_email],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 3,
    },
    description="DAG for feature monitoring.",
    doc_md=__doc__,
    schedule_interval=None,
    start_date=datetime.datetime(2022, 10, 30, 10),
    catchup=False,
    tags=["critical"],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")

    create_main_stats_table = PostgresOperator(
        task_id="create_main_stats_table",
        sql="""
                CREATE TABLE IF NOT EXISTS main_data_stats (
                data_id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                dataset_name VARCHAR NOT NULL,
                row_count VARCHAR NOT NULL,
                column_count DATE NOT NULL,
                OWNER VARCHAR NOT NULL);
              """,
    )

    create_feature_stat_table = PostgresOperator(
        task_id="create_feature_stat_table",
        sql="""
                CREATE TABLE IF NOT EXISTS features_stats (
                data_id SERIAL PRIMARY KEY,
                dataset_name VARCHAR NOT NULL,
                date TIMESTAMP,
                {', '.join([(x) + ' FLOAT' for x in config.sql_mean_columns])},
                {', '.join([(x) + ' FLOAT' for x in config.sql_std_columns])},
                {', '.join([(x) + ' FLOAT' for x in config.sql_missing_columns])};
              """,
    )

    end_task = EmptyOperator(task_id="end")

    start_task >> create_main_stats_table >> create_feature_stat_table >> end_task
