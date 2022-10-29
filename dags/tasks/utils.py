"""utils module."""""


def get_spark(app_name=None):
    import pyspark
    from pyspark import SparkConf

    app_name = app_name or "spark_app"
    conf = SparkConf()
    conf.setAppName(app_name)
    conf.set("spark.dynamicAllocation.enabled", "true")
    conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    conf.set("spark.driver.maxResultSize", "4G")
    conf.set("spark.driver.memory", "4G")
    conf.set("spark.executor.memory", "4G")
    conf.set("spark.driver.allowMultipleContexts", "true")

    spark = pyspark.sql.SparkSession.builder.config(conf=conf).getOrCreate()
    return spark


if __name__ == "__main__":
    pass
