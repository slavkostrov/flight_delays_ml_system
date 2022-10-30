import os
import zipfile

import boto3
import mlflow
import pandas as pd
from pyspark.sql import SparkSession

if __name__ == "__main__":
    session = boto3.session.Session()
    s3 = session.client(
        service_name='s3',
        endpoint_url=os.getenv("AWS_S3_ENDPOINT_URL")

    )
    s3.download_file(os.getenv("BUCKET_NAME"), os.getenv("MODELS_PATH"), "./models.zip")
    with zipfile.ZipFile("./models.zip", 'r') as zip_ref:
        zip_ref.extractall("./")

    spark = (
        SparkSession
            .builder
            .master("local[1]")
            .appName('predict')
            .getOrCreate()
    )

    pipeline = mlflow.spark.load_model("./home/ubuntu/final_project/models/pipeline")
    model = mlflow.spark.load_model("./home/ubuntu/final_project/models/regression")

    s3.download_file(os.getenv("BUCKET_NAME"), os.getenv("DATA_PATH"), "./data.csv")

    columns = [
        'DEP_DELAY', 'TAXI_OUT', 'CRS_ELAPSED_TIME',
        'DISTANCE', 'flight_weekday_1', 'flight_weekday_2',
        'flight_weekday_3', 'flight_weekday_4', 'flight_weekday_5',
        'flight_weekday_6', 'flight_weekday_7'
    ]
    data = pd.read_csv("./data.csv")
    for day in range(1, 8):
        data[f"flight_weekday_{day}"] = (pd.to_datetime(data["FL_DATE"]).dt.dayofweek == day).astype(int)
    print(f"data shape: {data.shape}")
    sdf = spark.createDataFrame(data[columns])

    predictions = model.transform(pipeline.transform(sdf)).toPandas()
    predictions.to_csv("preds.csv", index=False)
    s3.upload_file("preds.csv", os.getenv("BUCKET_NAME"), os.getenv("OUTPUT_PATH"))
