"""
Module with tasks for feature generation
"""
import datetime
import sys

_folder = __file__[: __file__.rfind("/") + 1]
sys.path.extend([_folder, _folder[:_folder.rfind("/") + 1]])

from utils import get_spark, read_parquet
from base_config import Config

from airflow.models import Variable
import pyspark.sql.functions as F

import logging

logger = logging.getLogger("prepare_features")


def collect_raw_data(config: Config):
    """
    Collect raw data from source. In real production it must be some data scrapping,
    but in study case we use ready dataset.

    :param config: Config object (see base_config.py)
    :return:
    """
    spark = get_spark(app_name=f"{config.dag_name}/collect_raw_data")
    from_date = Variable.get("max_available_date", None)
    to_date = datetime.datetime.now()
    if from_date is None:
        from_date = (
                to_date
                - datetime.timedelta(days=config.max_history_days)
        )
        from_date = str(from_date.date())
    to_date = str(to_date.date())
    logger.info(f"FROM_DATE: {from_date} (used in filter).")
    logger.info(f"TO_DATE: {to_date} (used in filter).")

    data_path = f"{config.output_prefix}/raw_data.parquet"
    data = read_parquet(spark, data_path)

    data = data.filter(
        (F.col(config.date_dk) >= from_date)
        & (F.col(config.date_dk) <= to_date)
    )

    output_path = f"{config.output_prefix}/fresh_data_part.parquet"
    logger.info(f"Writing result into {output_path}.")
    data.write.parquet(output_path, mode="overwrite")
    Variable.set("max_available_date", to_date)


def clean_data(config: Config):
    """
    Delete useless rows/columns from data.

    :param config:  Config object (see base_config.py)
    :return:
    """
    spark = get_spark(app_name=f"{config.dag_name}/collect_raw_data")

    data_path = f"{config.output_prefix}/fresh_data_part.parquet"
    data = read_parquet(spark, data_path)

    columns_diff = set(config.required_columns) - set(data.columns)
    if len(columns_diff) > 0:
        raise RuntimeError(f"Missing columns in data! Namely: {columns_diff}.")

    count_before = data.count()
    data = (
        data
            .filter(F.col(config.target_column).isNotNull())
    )
    count = data.count()
    logger.info(f"Filtered zero targets. Rows before: {count_before}, after: {count}.")

    output_path = f"{config.output_prefix}/clean_data_part.parquet"
    logger.info(f"Writing result into {output_path}.")
    data.write.parquet(output_path, mode="overwrite")


def concat_features(config: Config):
    """
    Add new data delta to already calculated features.

    :param config:  Config object (see base_config.py)
    :return:
    """
    spark = get_spark(app_name=f"{config.dag_name}/concat_features")
    features_path = f"{config.output_prefix}/features_{config.dataset_name}.parquet"

    features = None
    try:
        features = read_parquet(spark, features_path)
    except Exception as e:
        logger.error(str(e))
        logger.info("Empty features, creating fresh features.")

    data = read_parquet(spark, f"{config.output_prefix}/clean_data_part.parquet")

    if features and set(data.columns) != set(features.columns):
        raise RuntimeError(f"Different data formats, new data has:\n{data.columns}\nold:\n{features.columns}")

    if features is None:
        features = data
    else:
        features = features.union(data)

    output_path = f"{config.output_prefix}/_features_{config.dataset_name}.parquet"
    logger.info(f"Writing result into {output_path}.")
    features.write.parquet(output_path, mode="overwrite")
    
    # TODO: FIX with paritions or smth else
    features_path = f"{config.output_prefix}/features_{config.dataset_name}.parquet"
    features = spark.read.parquet(output_path)
    features.write.parquet(features_path, mode="overwrite")

