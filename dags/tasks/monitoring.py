"""
Module with tasks for features monitoring.
"""
import datetime
import sys

_folder = __file__[: __file__.rfind("/") + 1]
sys.path.extend([_folder, _folder[:_folder.rfind("/") + 1]])

from utils import get_spark, read_parquet
from base_config import Config

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
