import typing as tp

from pydantic import BaseModel


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
