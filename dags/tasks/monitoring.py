"""
Module with tasks for features monitoring.
"""
import datetime
import sys

_folder = __file__[: __file__.rfind("/") + 1]
sys.path.extend([_folder, _folder[:_folder.rfind("/") + 1]])

from utils import get_spark, read_parquet, setup_s3_credentials
from base_config import Config
import pyspark.sql.functions as F
import mlflow

import logging

logger = logging.getLogger("monitoring")


def get_hook():
    from airflow.providers.postgres.hooks.postgres import PostgresHook
    hook = PostgresHook()
    return hook


def update_main_stats(config: Config):
    """
    Schema:
                CREATE TABLE IF NOT EXISTS main_data_stats (
                data_id SERIAL PRIMARY KEY,
                date TIMESTAMP,
                dataset_name VARCHAR NOT NULL,
                row_count VARCHAR NOT NULL,
                column_count DATE NOT NULL,
                OWNER VARCHAR NOT NULL);
    """
    spark = get_spark(f"{config.dag_name}/update_main_stats")
    data = read_parquet(spark, f"{config.output_prefix}/fresh_data_part.parquet")

    date = datetime.datetime.now()
    dataset_name = config.dataset_name
    row_count = data.count()
    columns_count = len(data.columns)
    owner = config.dag_name

    hook = get_hook()
    values = (date, dataset_name, row_count, columns_count)
    logger.info(f"Inserting values: {values}")
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                """
                INSERT INTO main_data_stats (date, dataset_name, row_count, column_count)
                VALUES (%s, %s, %s, %s);
                """,
                values
            )


def update_features_stats(config: Config):
    """
    Schema:
                    CREATE TABLE IF NOT EXISTS features_stats (
                    data_id SERIAL PRIMARY KEY,
                    dataset_name VARCHAR NOT NULL,
                    date TIMESTAMP,
                    {', '.join([(x) + ' FLOAT' for x in config.sql_mean_columns])},
                    {', '.join([(x) + ' FLOAT' for x in config.sql_std_columns])},
                    {', '.join([(x) + ' FLOAT' for x in config.sql_missing_columns])});
    """
    spark = get_spark(f"{config.dag_name}/update_features_stats")
    data = read_parquet(spark, f"{config.output_prefix}/fresh_data_part.parquet")

    agg_columns = []
    for column in config.input_features:
        agg_columns.append(F.mean(column).alias(f"mean_{column}"))
        agg_columns.append(F.stddev(column).alias(f"std_{column}"))
        agg_columns.append(F.sum(F.col(column).isNull().cast("int")).alias(f"nan_count_{column}"))
        agg_columns.append(F.mean(F.col(column).isNull().cast("int")).alias(f"nan_prop_{column}"))
    
    logger.info(f"Columns - {agg_columns}")
    all_columns = ["dataset_name", "date"] + config.sql_mean_columns + config.sql_std_columns + config.sql_missing_columns
    all_columns_sql = \
                f"""
                INSERT INTO features_stats ({', '.join(all_columns)})
                VALUES ({', '.join(['%s'] * len(all_columns))});
                """

    data_stats = data.select(agg_columns).toPandas()
    values = [config.dataset_name, datetime.datetime.now()] + data_stats[all_columns[2:]].iloc[0].values.tolist()

    logger.info("Inserting values.")
    hook = get_hook()
    with hook.get_conn() as conn:
        with conn.cursor() as cursor:
            cursor.execute(
                all_columns_sql,
                values
            )


def add_new_metrics(config: Config):
    setup_s3_credentials()
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)

    experiment_name = f"{config.dag_prefix}flight_delay_model"
    current_experiment = dict(mlflow.get_experiment_by_name(experiment_name))
    experiment_id = current_experiment['experiment_id']

    df = mlflow.search_runs([experiment_id], order_by=["Date DESC"])
    last_run_id = df.loc[0, 'run_id']
    last_run = mlflow.get_run(run_id=last_run_id)

    metrics_sql = \
        f"""
                    INSERT INTO model_metrics (date, model_name, dataset_name, metric_name, value)
                    VALUES (%s, %s, %s, %s, %s);
                    """

    hook = get_hook()
    date = datetime.datetime.now()
    for name, value in last_run.data.metrics.items():
        with hook.get_conn() as conn:
            values = (date, config.model_name, config.dataset_name, name, value)
            logger.info(f"Inserting: {values}")
            with conn.cursor() as cursor:
                cursor.execute(
                    metrics_sql,
                    values
                )