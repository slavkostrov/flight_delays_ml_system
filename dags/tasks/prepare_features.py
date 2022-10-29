"""
Module with tasks for feature generation
"""
import sys

sys.path.append(__file__[: __file__.rfind("/") + 1])
from utils import get_spark


def collect_raw_data(config=None):
    spark = get_spark(app_name="collect_raw_data")
    return None


def clean_data():
    return None


def concat_features():
    return None
