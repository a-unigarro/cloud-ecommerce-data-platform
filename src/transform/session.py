from pyspark.sql import SparkSession


def get_spark(app_name: str = "olist-platform") -> SparkSession:
    return (
        SparkSession.builder
        .master("local[*]")                                   # use all local cores, no cluster
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "8")          # default 200 is absurd for 100 MB
        .config("spark.sql.session.timeZone", "UTC")          # avoid machine-dependent timestamps
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")  # overwrite only touched partitions
        .getOrCreate()
    )