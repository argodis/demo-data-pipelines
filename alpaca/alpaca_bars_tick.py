# coding: utf-8

import argparse
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
LOCAL = False
DEBUG = False
CONFIG_FILE = "/dbfs/Users/david@argodis.de/github/demo/alpaca.ini"


#
# Are we running in Databricks or locally?
#
try:
    spark
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Alpaca tick bars").getOrCreate()
    DEBUG = True
    LOCAL = True
    CONFIG_FILE = "/Users/davidhoeppner/Work/argodis/git/demo-data-pipelines/alpaca/alpaca.ini"
else:
    DB_CLUSTER_ID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    environment = "local.trades" if LOCAL else "databricks.trades"
    trades_clean_path = config[environment]["TradesClean"]
    bar_storage_path = config[environment]["BarsTicksPath"]

    parser = argparse.ArgumentParser(
        description="Alpaca volume bars."
    )

    # Sample down size
    parser.add_argument(
        "--sample-size",
        type=int,
        dest="size",
        default=1_000,
        help="Size of the sample"
    )

    # Number of bars wanted
    parser.add_argument(
        "--number-bars",
        type=int,
        dest="number_bars",
        help="Number of bars data is sampled down"
    )

    args = parser.parse_args()

    # TODO: calculate sample_size from volume and number_of_bars
    sample_size = args.size

    df = spark.read.parquet(trades_clean_path)

    df = df.withColumn(
        "x_row_number",
        F.row_number().over(
            Window.partitionBy("symbol", "date").orderBy(F.col("timestamp"))))

    df = df.withColumn(
        "x_count",
        F.count("symbol").over(
            Window.partitionBy("symbol", "date")))

    if args.number_bars:
        bar_storage_path += f"/{args.number_bars}b"
        df = df.withColumn(
            "x_sample_size",
            F.ceil(F.col("x_count") / args.number_bars) + 1
        )

        df = df.withColumn(
            "x_div",
            F.expr(f"x_row_number div x_sample_size")
        )
    else:
        bar_storage_path += f"/{sample_size}"

        df = df.withColumn(
            "x_div",
            F.expr(f"x_row_number div {sample_size}")
        )

    df.show()

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

    df.show()

    df.coalesce(2).write.mode('overwrite').parquet(bar_storage_path)
