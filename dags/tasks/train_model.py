"""
Module with tasks for model training
"""
import datetime
import sys

from pyspark.ml import Pipeline

_folder = __file__[: __file__.rfind("/") + 1]
sys.path.extend([_folder, _folder[:_folder.rfind("/") + 1]])

from utils import get_spark, setup_s3_credentials, read_parquet
from base_config import Config

import mlflow
from pyspark.ml.regression import LinearRegression
from pyspark.ml.evaluation import RegressionEvaluator
from pyspark.ml.feature import VectorAssembler
from pyspark.ml.feature import Imputer
import pyspark.sql.functions as F
import logging

logger = logging.getLogger("train_model")

feature_columns = [
    'DEP_DELAY',
    'TAXI_OUT',
    'CRS_ELAPSED_TIME',
    'DISTANCE',
    'flight_weekday_1',
    'flight_weekday_2',
    'flight_weekday_3',
    'flight_weekday_4',
    'flight_weekday_5',
    'flight_weekday_6',
    'flight_weekday_7'
]


def train_model(config: Config):
    """
    Train model and track it with MlFlow.

    :param config: Config object (see base_config.py)
    :return:
    """
    spark = get_spark(app_name=f"{config.dag_name}/train_model")
    setup_s3_credentials()

    # enable pyspark autologs, metrics logging disabled for custom names
    mlflow.pyspark.ml.autolog(log_post_training_metrics=True)
    # set tracking uri (localhost for education)
    mlflow.set_tracking_uri(config.mlflow_tracking_uri)

    today = str(datetime.datetime.now().date())
    model_name = f"{config.model_name}_{today}"
    pipeline_name = f"pipeline_{config.model_name}_{today}"

    # set exp name
    mlflow.set_experiment(f"{config.dag_prefix}flight_delay_model")
    with mlflow.start_run(description="flight_delay_model_evaluation") as active_run:
        features = read_parquet(spark, f"{config.output_prefix}/features_{config.dataset_name}.parquet")
        features = _add_weekdays_features(features)
        features = features.select(feature_columns + [config.target_column])
        features = features.filter(F.col(config.target_column).isNotNull())

        output_columns = ["{}_imputed".format(c) for c in feature_columns]
        logger.info(f"Using features: {output_columns}")
        imputer = Imputer(
            inputCols=feature_columns,
            outputCols=output_columns,
        ).setStrategy("mean")
        assembler = VectorAssembler(inputCols=output_columns, outputCol="features")
        transformer = Pipeline(stages=[imputer, assembler])

        logger.info("Fitting Transformer.")
        transformer = transformer.fit(features)
        logger.info("Transforming Features.")
        features = transformer.transform(features)

        model = LinearRegression(featuresCol="features", labelCol=config.target_column)
        # Splitting into train and test datasets
        train, test = features.randomSplit([0.8, 0.2])
        logger.info(f"train size: {train.count()}, test size: {test.count()}.")

        logger.info(f"Fitting model {model}.")
        model = model.fit(train)

        # Predicting and Finding R2 and RMSE Values
        predictions = model.transform(test)
        eval_reg = RegressionEvaluator(labelCol=config.target_column, metricName="r2")
        eval_results = model.evaluate(test)

        logger.info("R Squared (R2) on test data = %g" % eval_reg.evaluate(predictions))
        logger.info("Root Mean Squared Error (RMSE) on test data = %g" % eval_results.rootMeanSquaredError)

        # save model & pipeline to artifacts store
        mlflow.spark.log_model(model, "regression", registered_model_name=model_name)
        mlflow.spark.log_model(model, "pipeline", registered_model_name=pipeline_name)

    logger.info(f"Writing train and test DF into {config.output_prefix}/datasets/{config.dataset_name}/")
    train.write.parquet(f"{config.output_prefix}/datasets/{config.dataset_name}/train.parquet", mode="OVERWRITE")
    test.write.parquet(f"{config.output_prefix}/datasets/{config.dataset_name}/test.parquet", mode="OVERWRITE")


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


def _add_weekdays_features(df):
    for day in range(1, 8):
        df = df.withColumn(f"flight_weekday_{day}", (F.dayofweek("FL_DATE") == F.lit(day)).cast("int"))
    return df
