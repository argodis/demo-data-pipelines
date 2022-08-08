"""
Download bars from Alpaca
"""


import argparse
import configparser
import datetime
import os
import logging
import sys
import pytz

from pathlib import Path

from tqdm import tqdm

import pandas as pd

from alpaca_trade_api.rest import REST
from alpaca_trade_api.rest import URL
from alpaca_trade_api.rest import TimeFrame
from alpaca_trade_api.rest import TimeFrameUnit
from alpaca_trade_api.rest import APIError


ALPACA_INTERVAL = TimeFrame(1, TimeFrameUnit.Day)
ALPACA_ADJUSTMENT = "raw"
TODAY = datetime.datetime.now().strftime("%Y-%m-%d")
CONFIG_FILE = "/Users/davidhoeppner/Work/argodis/git/demo-data-pipelines/bars/downloader.ini"
#CONFIG_FILE = "/dbfs/Users/david@argodis.de/github/demo/downloader.ini"
LOCAL = True

if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Alpaca bars downloader."
    )

    # Symbols
    parser.add_argument("--symbols", type=str, help="Symbols to download data for")
    parser.add_argument("--symbols-by-volume", type=int, help="Select which symbols are download by minimum daily volume")

    # Time intervals
    parser.add_argument("--minute-bars", action="store_true", help="Download minute bars")

    # Adjustments
    parser.add_argument("--adjustment-raw", action="store_true", help="Raw bar adjustment")
    parser.add_argument("--adjustment-xxx", action="store_true", help="xxx bar adjustment")

    # Time
    parser.add_argument(
        "--start-date",
        type=str,
        dest="start_date",
        help="Start date",
    )

    parser.add_argument(
        "--end-date",
        type=str,
        dest="end_date",
        help="End date",
    )

    args = parser.parse_args()

    if args.symbols:
        if args.symbols_by_volume:
            logging.error("Select either --symbols or --symbols-by-volume")
            sys.exit(1)

        args.symbols = args.symbols.split(",")

    if args.minute_bars:
        ALPACA_INTERVAL = TimeFrame(1, TimeFrameUnit.Minute)

    if args.start_date:
        try:
            datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            print("Can not parse --start-date")
            sys.exit(1)
        else:
            ALPACA_START = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        ALPACA_START = datetime.datetime.now(pytz.timezone("EST")) - datetime.timedelta(1)

 
    if args.end_date:
        if not args.start_date:
            print("End date but no start date")
            sys.exit(1)
        try:
            datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            print("Can not parse date")
            sys.exit(1)
        else:
            ALPACA_END = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
    else:
        ALPACA_END = ALPACA_START + datetime.timedelta(1)

    assert(ALPACA_START)
    assert(ALPACA_END)

    ALPACA_START = ALPACA_START.strftime("%Y-%m-%d")
    ALPACA_END = ALPACA_END.strftime("%Y-%m-%d")

    print(ALPACA_START, ALPACA_END)

    key_id = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")

    PAPER_URL = "https://paper-api.alpaca.markets"
    paper_api = REST(key_id=key_id, secret_key=secret_key, base_url=URL(PAPER_URL))

    config = configparser.ConfigParser()
    config.read(CONFIG_FILE)

    environment = "local.downloader" if LOCAL else "databricks.downloader"
    daily_bars_path = config[environment]["DailyBarsPath"]
    storage_path = config[environment]["StoragePath"]

    STORAGE_DIR = Path(storage_path)

    if args.symbols:
        symbols = args.symbols
    elif args.symbols_by_volume:
        VOLUME = args.symbols_by_volume
        df = pd.read_parquet(daily_bars_path)
        aggregations = {
            'volume':'min'
        }
        df = df.groupby("symbol").agg(aggregations)
        df = df[df["volume"] > VOLUME]
        df = df.reset_index()
        symbols = df["symbol"].tolist()
        print(symbols)
    else:
        symbols = [el.symbol for el in paper_api.list_assets(status="active")]

    api = REST(key_id=key_id, secret_key=secret_key)

    for symbol in tqdm(symbols):
        p = STORAGE_DIR /f"{ALPACA_INTERVAL.amount}{ALPACA_INTERVAL.unit.name[0].lower()}" / f"symbol={symbol}/"
        p.mkdir(parents=True, exist_ok=True)
        p /= "00001.parquet"

        if Path(p).is_file():
            continue

        try:
            df = api.get_bars(
                symbol,
                ALPACA_INTERVAL,
                ALPACA_START,
                ALPACA_END,
                limit=1000000,
                adjustment=ALPACA_ADJUSTMENT,
            ).df
        except APIError:
            logging.error("Exception %s", symbol)
            continue

        df.to_parquet(p)
