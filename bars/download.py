"""
Download bars from Alpaca.

If no date range is given by the start-date and
end-date cmd line arguments we use a today semantics
as defined in the user/system timezone running the
script. We will NOT start partial downloads if the
market is still open.

Should be refactored with the Ray and
snapshot code. We also redownload data
because we do not know the filepath
when saving.
"""

import argparse
import datetime
import os
import logging
import sys
from pathlib import Path
import pytz

from tqdm import tqdm

import pandas as pd

from alpaca_trade_api.rest import REST
from alpaca_trade_api.rest import URL
from alpaca_trade_api.rest import TimeFrame
from alpaca_trade_api.rest import TimeFrameUnit
from alpaca_trade_api.rest import APIError


try:
    DB_CLUSTER_ID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId") # pylint: disable=E0602
except NameError:
    DB_CLUSTER_ID = None
    LOCAL = True
else:
    LOCAL = False


ALPACA_INTERVAL = TimeFrame(1, TimeFrameUnit.Day)
ALPACA_ADJUSTMENT = "raw"
TODAY = datetime.datetime.now().strftime("%Y-%m-%d")


if __name__ == "__main__":

    logging.basicConfig(level=logging.INFO)

    if DB_CLUSTER_ID:
        key_id = dbutils.secrets.get("dbc", "alpaca-key-id-unlimited") # pylint: disable=E0602
        secret_key = dbutils.secrets.get("dbc", "alpaca-key-secret-unlimited") # pylint: disable=E0602
    else:
        key_id = os.getenv("APCA_API_KEY_ID")
        secret_key = os.getenv("APCA_API_SECRET_KEY")

    parser = argparse.ArgumentParser(
        description="Alpaca bars downloader."
    )

    # Symbols
    parser.add_argument("--symbols", type=str, help="Symbols to download data for")
    parser.add_argument(
        "--symbols-by-volume",
        type=int, help="Select which symbols are download by minimum daily volume")

    # Time intervals
    parser.add_argument("--minute-bars", action="store_true", help="Download minute bars")

    # Adjustments
    parser.add_argument("--adjustment-raw", action="store_true", help="Raw bar adjustment")
    parser.add_argument("--adjustment-split", action="store_true", help="Split bar adjustment")
    parser.add_argument("--adjustment-divided", action="store_true", help="Divided bar adjustment")
    parser.add_argument("--adjustment-all", action="store_true", help="All bar adjustment")

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

    parser.add_argument(
        "--force",
        action="store_true",
        help="Force partial data download."
    )

    # Storage
    parser.add_argument(
        "--delta-table",
        type=str,
        dest="delta_table", help="Save dataframes to a delta table")
    parser.add_argument(
        "--storage-path",
        type=str, required=True,
        dest="storage_path", help="Under which path data should be stored")

    # Daily bars
    parser.add_argument(
        "--daily-bars-path",
        type=str,
        dest="daily_bars_path", help="Under which path data should be stored")

    args = parser.parse_args()

    PAPER_URL = "https://paper-api.alpaca.markets"
    paper_api = REST(key_id=key_id, secret_key=secret_key, base_url=URL(PAPER_URL))

    today = datetime.datetime.now()
    logging.warning("System is using timezone: %s", today.astimezone().tzname())

    timezone_est = pytz.timezone("EST")
    timezone_utc = pytz.timezone("UTC")
    now = datetime.datetime.now().astimezone()

    if args.symbols:
        if args.symbols_by_volume:
            logging.error("Select either --symbols or --symbols-by-volume")
            sys.exit(1)

        args.symbols = args.symbols.split(",")

    if args.minute_bars:
        ALPACA_INTERVAL = TimeFrame(1, TimeFrameUnit.Minute)

    if args.adjustment_split:
        ALPACA_ADJUSTMENT = "split"
    elif args.adjustment_divided:
        ALPACA_ADJUSTMENT = "divided"
    elif args.adjustment_all:
        ALPACA_ADJUSTMENT = "all"

    if args.start_date:
        try:
            datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            logging.error("Can not parse --start-date")
            sys.exit(1)
        else:
            ALPACA_START = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
    else:
        calendar = paper_api.get_calendar(start=today, end=today)
        if not calendar:
            logging.error("No calendar data for %s", today)
            sys.exit(1)

        calendar = calendar[0]

        date = calendar.date.to_pydatetime()
        market_open = date + datetime.timedelta(
            hours=calendar.open.hour,
            minutes=calendar.open.minute
        )
        market_close = date + datetime.timedelta(
            hours=calendar.close.hour,
            minutes=calendar.close.minute
        )

        market_close_tz = market_close.astimezone(timezone_est)
        market_open_tz = market_open.astimezone(timezone_est)

        if now < market_open_tz:
            logging.warning("Market not open yet.")
            if not args.force:
                logging.error("Exiting.")
                sys.exit(1)
        elif now < market_close_tz:
            logging.warning("Market is still open.")
            if not args.force:
                logging.error("Exiting.")
                sys.exit(1)

        ALPACA_START = market_open_tz.astimezone(timezone_utc)
        ALPACA_END = market_close_tz.astimezone(timezone_utc)

    if args.end_date:
        if not args.start_date:
            logging.error("End date but no start date")
            sys.exit(1)
        try:
            datetime.datetime.strptime(args.end_date, "%Y-%m-%d")
        except ValueError:
            logging.error("Can not parse date")
            sys.exit(1)
        else:
            ALPACA_END = datetime.datetime.strptime(args.end_date, "%Y-%m-%d")

    DELTA_TABLE = args.delta_table

    assert ALPACA_START
    assert ALPACA_END
    assert ALPACA_INTERVAL

    ALPACA_START = ALPACA_START.strftime("%Y-%m-%d")
    ALPACA_END = ALPACA_END.strftime("%Y-%m-%d")

    logging.info(ALPACA_START, ALPACA_END)

    STORAGE_DIR = Path(args.storage_path)

    if args.symbols:
        symbols = args.symbols
    elif args.symbols_by_volume:
        if not args.daily_bars_path:
            logging.error("--symbols-by-volume requires --daily_bars_path")
            sys.exit(1)
        VOLUME = args.symbols_by_volume
        df = pd.read_parquet(args.daily_bars_path)
        aggregations = {
            "v": "min"
        }
        df = df.groupby("symbol").agg(aggregations)
        df = df[df["v"] > VOLUME]
        df = df.reset_index()
        symbols = df["symbol"].tolist()
        logging.info(symbols)
    else:
        symbols = [el.symbol for el in paper_api.list_assets(status="active")]

    api = REST(key_id=key_id, secret_key=secret_key)

    p = STORAGE_DIR /f"{ALPACA_INTERVAL.amount}{ALPACA_INTERVAL.unit.name[0].lower()}"
    p.mkdir(parents=True, exist_ok=True)

    for symbol in tqdm(symbols):

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

        if df.empty:
            logging.warning("Skiping empty dataframe %s", symbol)
            continue

        df = df.reset_index()
        df["date"] = df["timestamp"].dt.date
        df["symbol"] = symbol
        df.to_parquet(p, partition_cols=["date", "symbol"])

        if DELTA_TABLE:
            spark_df = spark.createDataFrame(df) # pylint: disable=E0602
            spark_df.write.format("delta").mode("append").saveAsTable(DELTA_TABLE)
