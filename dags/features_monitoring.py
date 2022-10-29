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

from base_config import Config
from tasks.prepare_features import collect_raw_data, clean_data, concat_features

config = Config()

with DAG(
        f"{config.dag_prefix}feature_monitoring",
        default_args={
            'depends_on_past': False,
            'email': [config.user_email],
            'email_on_failure': True,
            'email_on_retry': True,
            'retries': 3,
        },
        description="DAG for feature monitoring.",
        doc_md=__doc__,
        schedule_interval=None,
        start_date=datetime.datetime(2022, 10, 30, 10),
        catchup=False,
        tags=['critical'],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    start_task >> end_task