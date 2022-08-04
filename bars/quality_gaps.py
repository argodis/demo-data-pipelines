"""
Find gaps in minute bar data.
"""

import argparse
import logging
import sys

from uniplot import plot

import numpy as np
import pandas as pd


STORAGE_PATH = "/tmp/datalake/landing/alpaca/bars/minute"


pd.set_option('display.max_rows', None)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Quality check time bars."
    )

    parser.add_argument(
        "--storage-path",
        type=str,
        dest="storage_path",
        help="Path to load data from",
    )

    args = parser.parse_args()

    if args.storage_path:
        STORAGE_PATH = args.storage_path

    try:
        df = pd.read_parquet(STORAGE_PATH)
    except FileNotFoundError:
        logging.error("Can not load data from %s", STORAGE_PATH)
        sys.exit(1)

    df = df.between_time("9:30", "17:45")
    df = df.reset_index()
    df["delta"] = df.groupby("symbol")["timestamp"].diff()
    df = df.where(df["delta"] > np.timedelta64(1, "m")).dropna()

    df["seconds"] = df["delta"].dt.seconds

    plot_df = df[["timestamp", "symbol", "seconds"]].copy()

    plot_df.set_index(plot_df["timestamp"], inplace = True)
    #plot_df = plot_df.drop(columns=["timestamp"])

    for i, g in plot_df.groupby(pd.Grouper(freq='24H')):
        if not g.empty:
            g = g.where(g["seconds"] < 1000).dropna()
            plot(ys=g["seconds"])
