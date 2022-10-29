"""
Module with tasks for feature generation
"""
from utils import get_spark


def collect_raw_data(config):
    spark = get_spark(app_name="collect_raw_data")
    return None


def clean_data():
    return None


def concat_features():
    return None
