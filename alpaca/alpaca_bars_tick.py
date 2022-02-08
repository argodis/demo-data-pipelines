# coding: utf-8

import configparser
import sys

import pyspark.sql.functions as F

from pyspark.sql import Window

#
# Part of the daily executed job to download Alpaca trading data.
#

#sys.path.insert(0, "/dbfs/Users/david@argodis.de/github/demo/")

#
# Global variables
#

DB_CLUSTER_ID = None
LOCAL = True
#CONFIG_FILE = "/Users/davidhoeppner/Work/argodis/git/demo-data-pipelines/alpaca/alpaca.ini"
CONFIG_FILE = "/dbfs/Users/david@argodis.de/github/demo/alpaca.ini"


#
# Are we running in Databricks or locally?
#
try:
    spark
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Alpaca tick bars").getOrCreate()
else:
    DB_CLUSTER_ID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    LOCAL = False


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    environment = "local.trades" if LOCAL else "databricks.trades"
    trades_clean_path = config[environment]["TradesClean"]
    bar_storage_path = config[environment]["BarsTicksPath"]
    bar_storage_path += "/100"


    df = spark.read.parquet(trades_clean_path)

    df = df.withColumn(
        "x_row_number",
        F.row_number().over(
            Window.partitionBy("symbol", "date").orderBy(F.col("timestamp"))))

    df = df.withColumn(
        "x_div",
        F.expr("x_row_number div 100")
    )

    df = df.groupBy(
        df.symbol,
        df.date,
        df.x_div
    ).agg(
        F.max(df.price).alias("max_price"),
        F.min(df.price).alias("min_price"),
        F.count(df.price).alias("count"),
        F.first(df.price).alias("first_price"),
        F.last(df.price).alias("last_price")
    )

    df.coalesce(2).write.mode('overwrite').parquet(bar_storage_path)
