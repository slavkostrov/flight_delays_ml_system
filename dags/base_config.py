import typing as tp

from pydantic import BaseModel, EmailStr, HttpUrl


class Config(BaseModel):
    """
    Base config for all ML pipeline DAGs.
    """

    # hdfs path of main production folder
    output_prefix: tp.AnyStr = "hdfs://home/ubuntu/pipeline"

    # dataset name of current run, can be replaced by any
    dataset_name: tp.AnyStr = "prod_ds"

    # same, but model name
    model_name: tp.AnyStr = "prod_model"

    # max train dataset size (limitation of resources)
    max_train_size: int = 5_000_000

    # email of user
    user_email: EmailStr = "slavkostrov@gmail.com"

    # dag prefix
    dag_prefix: tp.AnyStr = "PROD_"

    # max date to use in train
    max_history_days: int = 100

    # date primary key
    date_dk: str = "FL_DATE"
    target_column: str = "DEP_DELAY"

    required_columns: tp.List[str] = [
        'FL_DATE',
        'OP_CARRIER',
        'OP_CARRIER_FL_NUM',
        'ORIGIN',
        'DEST',
        'CRS_DEP_TIME',
        'DEP_TIME',
        'DEP_DELAY',
        'TAXI_OUT',
        'WHEELS_OFF',
        'WHEELS_ON',
        'TAXI_IN',
        'CRS_ARR_TIME',
        'ARR_TIME',
        'ARR_DELAY',
        'CANCELLED',
        'DIVERTED',
        'CRS_ELAPSED_TIME',
        'ACTUAL_ELAPSED_TIME',
        'AIR_TIME',
        'DISTANCE'
    ]

    dag_name: str = ""
    mlflow_tracking_uri: HttpUrl = "http://localhost:5000"
