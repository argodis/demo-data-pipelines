# coding: utf-8

import configparser
import sys


#
# Part of the daily executed job to download Alpaca trading data.
#


#
# Global variables
#

#sys.path.insert(0, "/dbfs/Users/david@argodis.de/github/demo/")


DB_CLUSTER_ID = None
LOCAL = True
DEBUG = False
CONFIG_FILE = "/dbfs/Users/david@argodis.de/github/demo/alpaca.ini"

#
# Are we running in Databricks or locally?
#
try:
    spark
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.appName("Alpaca CLEAN").getOrCreate()
    spark.conf.set("spark.sql.session.timeZone", "EST")
    DEBUG = True
    CONFIG_FILE = "/Users/davidhoeppner/Work/argodis/git/demo-data-pipelines/alpaca/alpaca.ini"
else:
    DB_CLUSTER_ID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
    LOCAL = False


if __name__ == "__main__":
    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    environment = "local.trades" if LOCAL else "databricks.trades"

    trades_landing_path = config[environment]["TradesLanding"]
    trades_clean_path = config[environment]["TradesClean"]

    df = spark.read.parquet(trades_landing_path)

    if DEBUG:
        df.printSchema()
        df.sort("timestamp").show(truncate=False,vertical=True)

    df.repartition('date').write.mode('overwrite').partitionBy('date').parquet(trades_clean_path)

