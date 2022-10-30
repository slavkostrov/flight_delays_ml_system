"""
### Model train DAG.
#### Purpose
DAG takes prepared features and fit ML model.
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
from tasks.train_model import train_model, eval_model

config = Config()
DAG_NAME = f"{config.dag_prefix}train_model"
config.dag_name = DAG_NAME

with DAG(
    DAG_NAME,
    default_args={
        "depends_on_past": False,
        "email": [config.user_email],
        "email_on_failure": True,
        "email_on_retry": True,
        "retries": 3,
    },
    description="DAG for weekly model training.",
    doc_md=__doc__,
    schedule_interval=None,
    start_date=datetime.datetime(2022, 10, 28, 10),
    catchup=False,
    tags=["critical"],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")

    train_model_task = PythonOperator(
        python_callable=train_model, op_kwargs={"config": config}, task_id="train_model"
    )

    eval_model_task  = PythonOperator(
        python_callable=eval_model, op_kwargs={"config": config}, task_id="eval_model"
    )

    end_task = EmptyOperator(task_id="end")
    start_task >> train_model_task >> eval_model_task >> end_task
