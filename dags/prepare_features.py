"""
### Feature preparation DAG.
#### Purpose
DAG takes raw data and prepare it for next using in ML model fitting.
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
DAG_NAME = f"{config.dag_prefix}prepare_features"
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
        description="DAG for weekly feature preparation.",
        doc_md=__doc__,
        schedule_interval="@weekly",
        start_date=datetime.datetime(2022, 10, 28, 10),
        catchup=False,
        tags=["critical"],
) as dag:
    dag.doc_md = __doc__

    start_task = EmptyOperator(task_id="start")
    end_task = EmptyOperator(task_id="end")

    # collecting raw data from API / or (in this case) just simulated data
    collect_raw_data_task = PythonOperator(
        python_callable=collect_raw_data, op_kwargs={"config": config}, task_id="collect_raw_data"
    )

    clean_data_task = PythonOperator(
        python_callable=clean_data, op_kwargs={"config": config}, task_id="clean_data"
    )

    concat_features_task = PythonOperator(
        python_callable=concat_features,
        op_kwargs={"config": config},
        task_id="concat_features",
    )

    run_monitoring_dag = TriggerDagRunOperator(
        task_id="run_features_monitoring",
        trigger_dag_id=f"{config.dag_prefix}features_monitoring",
    )

    run_training_dag = TriggerDagRunOperator(
        task_id="run_model_train", trigger_dag_id=f"{config.dag_prefix}train_model"
    )

    start_task >> collect_raw_data_task >> clean_data_task >> concat_features_task

    concat_features_task >> [run_monitoring_dag, run_training_dag] >> end_task
