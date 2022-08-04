"""
Download bars from Alpaca
"""


import argparse
import os
import logging

from pathlib import Path

from tqdm import tqdm

from alpaca_trade_api.rest import REST
from alpaca_trade_api.rest import URL
from alpaca_trade_api.rest import TimeFrame
from alpaca_trade_api.rest import TimeFrameUnit
from alpaca_trade_api.rest import APIError


STORAGE_DIR = Path("/tmp/datalake/landing/alpaca/bars/")
ALPACA_INTERVAL = TimeFrame(1, TimeFrameUnit.Day)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Alpaca bars downloader."
    )

    # Symbols
    parser.add_argument("--symbols", type=str, help="Symbols to download data for")

    # Time intervals
    parser.add_argument("--minute-bars", action="store_true", help="Download minute bars")

    args = parser.parse_args()

    if args.symbols:
        args.symbols = args.symbols.split(",")

    if args.minute_bars:
        ALPACA_INTERVAL = TimeFrame(1, TimeFrameUnit.Minute)

    key_id = os.getenv("APCA_API_KEY_ID")
    secret_key = os.getenv("APCA_API_SECRET_KEY")

    PAPER_URL = "https://paper-api.alpaca.markets"
    paper_api = REST(key_id=key_id, secret_key=secret_key, base_url=URL(PAPER_URL))

    if not (symbols := args.symbols):
        symbols = [el.symbol for el in paper_api.list_assets(status="active")]

    api = REST(key_id=key_id, secret_key=secret_key)

    for symbol in tqdm(symbols):
        p = STORAGE_DIR /f"{ALPACA_INTERVAL.unit.name.lower()}" / f"symbol={symbol}/"
        p.mkdir(parents=True, exist_ok=True)
        p /= "00001.parquet"

        if Path(p).is_file():
            continue

        try:
            df = api.get_bars(
                symbol,
                ALPACA_INTERVAL,
                "2022-01-01",
                "2022-01-30",
                limit=1000000,
                adjustment="raw",
            ).df
        except APIError:
            logging.error("Exception %s", symbol)
            continue

        df.to_parquet(p)
