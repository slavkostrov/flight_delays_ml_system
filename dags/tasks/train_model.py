"""
Module with tasks for model training
"""
import datetime
import sys

_folder = __file__[: __file__.rfind("/") + 1]
sys.path.extend([_folder, _folder[:_folder.rfind("/") + 1]])

from utils import get_spark
from base_config import Config

import os

import mlflow
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
import pyspark.sql.functions as F
import logging

logger = logging.getLogger("train_model")

feature_columns = ['DEP_DELAY', 'TAXI_OUT', 'CRS_ELAPSED_TIME', 'DISTANCE',
                   'flight_weekday_1', 'flight_weekday_2', 'flight_weekday_3',
                   'flight_weekday_4', 'flight_weekday_5', 'flight_weekday_6',
                   'flight_weekday_7']


def train_model(config: Config):
    """
    Train model and track it with MlFlow.

    :param config: Config object (see base_config.py)
    :return:
    """
    spark = get_spark(app_name=f"{config.dag_name}/train_model")
    # enable pyspark autologs, metrics logging disabled for custom names
    mlflow.pyspark.ml.autolog(log_post_training_metrics=True)

    # set tracking uri (localhost for education)
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)

    today = str(datetime.datetime.now().date())
    model_name = f"{config.model_name}_{today}"

    # set exp name
    mlflow.set_experiment("flight_delay_model")
    with mlflow.start_run(run_name=model_name, description="flight_delay_model_evaluation") as active_run:
        features = spark.read.parquet(f"{config.output_prefix}/features_{config.dataset_name}.parquet")
        for day in range(1, 8):
            features = features.withColumn(f"flight_weekday_{day}", (F.dayofweek("FL_DATE") == F.lit(day)).cast("int"))

        assembler = VectorAssembler(inputCols=feature_columns, outputCol="features")

        flights_sdf_v2 = assembler.transform(features.dropna())
        flights_sdf_v2 = flights_sdf_v2.select(['features', 'ARR_DELAY'])

        # Splitting into train and test datasets
        train, test = flights_sdf_v2.randomSplit([0.8, 0.2])

        # Fitting Linear Regression Model
        lreg_v1 = LinearRegression(featuresCol="features", labelCol="ARR_DELAY")
        lr_model_v1 = lreg_v1.fit(train)

        # Predicting and Finding R2 and RMSE Values
        predictions_v1 = lr_model_v1.transform(test)
        eval_reg = RegressionEvaluator(labelCol="ARR_DELAY", metricName="r2")
        test_result_v1 = lr_model_v1.evaluate(test)

        logger.info("R Squared (R2) on test data = %g" % eval_reg.evaluate(predictions_v1))
        logger.info("Root Mean Squared Error (RMSE) on test data = %g" % test_result_v1.rootMeanSquaredError)

        mlflow.spark.log_model(lr_model_v1, "SparkML-linear-regression")
        return
        os.system("hdfs dfs -rm -r tmp_model")
        fitted_model.save("tmp_model")
        model_ex = LogisticRegressionModel.load("tmp_model")
        # temp model saving to local
        mlflow.spark.save_model(model_ex, "spark-model")
        # save model to object-storage as artifact
        mlflow.log_artifacts("spark-model")


def eval_model(config: Config):
    """
    Evaluate model and compare it with previous version.
    If new model is worst, production continue use old one.

    :param config: Config object (see base_config.py)
    :return:
    """
    pass


def move_model_to_s3(config: Config):
    """
    Take new best model and save it to S3.

    :param config: Config object (see base_config.py)
    :return:
    """
    pass
