# coding: utf-8

# RAY_record_ref_creation_sites=1

# 2022-01-24
#
#
#

#
# A ray workflow to download data from the Alpaca REST API.
#
# The workflow currently consist of only one workflow step.  This
# was done to minimise data serialization and reduce I/O operations
# on the Azure Blob Storage.  As Ray workflows need a shared
# read-write storage to exchange data between workers.  For a
# similar reason we also split data writes between cheaper
# Azure Blob Storage and Azure Data Lake.
#

import os
from pickle import NONE
import sys

from pathlib import Path


sys.path.insert(0, "/dbfs/Users/david@argodis.de/github/demo/")
sys.stdout.fileno = lambda: 0

#
# Global Settings
#
ALPACA_PARQUET_VERSION = "2.0"
ALPACA_RATE_LIMIT = 3
ALPACA_OFFSET = 8
ALPACA_STORAGE_DIR = "datalake/landing/alpaca"
RAY_WORKFLOW_DIR = ""

try:
    DB_CLUSTER_ID = spark.conf.get("spark.databricks.clusterUsageTags.clusterId")
except NameError:
    DB_CLUSTER_ID = None

if DB_CLUSTER_ID:
    WORKFLOW_STORAGE = "/dbfs/mnt/data/alpaca/workflows"
    STORAGE_DIR = Path("/dbfs/mnt/datalake/landing/alpaca")
else:
    WORKFLOW_STORAGE = "/tmp/data/alpaca/workflows"
    STORAGE_DIR = Path("/tmp/datalake/landing/alpaca")

STORAGE_DIR.mkdir(exist_ok=True, parents=True)


import ray
import datetime
import argparse
import random
import logging
import time

import pandas as pd

from pathlib import Path
from itertools import compress
from itertools import islice
from enum import Enum
from dataclasses import dataclass

from ray import workflow
from ray.actor import ActorHandle

from alpaca_trade_api.rest import REST
from alpaca_trade_api.rest import URL
from alpaca_trade_api.rest import APIError
from alpaca_trade_api.rest import RetryException
from alpaca_trade_api.rest import TimeFrame

from requests.exceptions import HTTPError


from ray_rate_limit_actor import LeakyBucketActor
from ray_telemetry_actor import TelemetryActor


class Kind(Enum):
    QUOTES = 1
    TRADES = 2
    BARS = 3


@dataclass
class StepPayload:
    asset: str
    kind: Kind
    start: str
    end: str
    format: str = "parquet"
    batch_id: int = 0

    def to_filename(self) -> str:
        p = STORAGE_DIR / f"{self.kind.name.lower()}/date={self.start}/symbol={self.asset}/"
        p.mkdir(parents=True, exist_ok=True)
        return str(p / f"{self.batch_id:0>5}.{self.format}")


class CustomREST(REST):
    def __init__(self, actor, key_id, secret_key):
        super().__init__(key_id, secret_key)
        self._actor = actor

    def _one_request(self, method: str, url: URL, opts: dict, retry: int):
        ray.get(self._actor.update.remote("XXX", 1))
        retry_codes = self._retry_codes
        resp = self._session.request(method, url, **opts)
        try:
            resp.raise_for_status()
        except HTTPError as http_error:
            # retry if we hit Rate Limit
            if resp.status_code in retry_codes and retry > 0:
                raise RetryException()
            if "code" in resp.text:
                error = resp.json()
                if "code" in error:
                    raise APIError(error, http_error)
            else:
                raise
        if resp.text != "":
            return resp.json()
        return None


@workflow.step
def extract(actor: ActorHandle, api, payload: StepPayload) -> None:
    file_name = payload.to_filename()
    asset = payload.asset
    day = payload.start
    kind = payload.kind

    start = time.time()

    # Check file exists and read file if so
    if Path(file_name).is_file():
        # df = pd.read_parquet(file_name)
        return
    else:
        # use switch in 3.10
        if kind == Kind.QUOTES:
            df = api.get_quotes(asset, day, day).df
        elif kind == Kind.TRADES:
            df = api.get_trades(asset, day, day).df
        elif kind == Kind.BARS:
            df = api.get_bars(
                asset,
                TimeFrame.Minute,
                pd.Timestamp("now").date(),
                pd.Timestamp("now").date(),
                limit=1,
                adjustment="raw",
            ).df

    duration = time.time() - start
    actor.add.remote(day, asset, duration)

    df.to_parquet(file_name, version=ALPACA_PARQUET_VERSION)


@workflow.step
def alpaca(args, key_id, secret_key, telemetry_actor):

    # maybe argparse can to that already
    kinds = list(
        compress(
            [Kind.QUOTES, Kind.TRADES, Kind.BARS], [args.quotes, args.trades, args.bars]
        )
    )

    if not kinds:
        kinds = [Kind.QUOTES]

    paper_url = "https://paper-api.alpaca.markets"
    paper_api = REST(key_id=key_id, secret_key=secret_key, base_url=URL(paper_url))

    if not (symbols := args.symbols):
        symbols = [el.symbol for el in paper_api.list_assets(status="active")]
        random.shuffle(symbols)

    print("Symbols: ", symbols)

    calendars = paper_api.get_calendar(start=args.start_date, end=args.end_date)

    alpaca_rate_limit = 3 if args.limited else 15
    actor = LeakyBucketActor.remote(rate=alpaca_rate_limit)

    print("Rate limit: ", alpaca_rate_limit)

    api = CustomREST(actor=actor, key_id=key_id, secret_key=secret_key)

    def group_elements(lst, chunk_size):
        lst = iter(lst)
        return iter(lambda: list(islice(lst, chunk_size)), [])

    workflow_parameters = []
    for calendar in calendars:
        day = calendar.date.strftime("%Y-%m-%d")
        for asset in symbols:
            for kind in kinds:
                payload = StepPayload(asset, kind, day, day)
                workflow_parameters.append(payload)

    for parameter_chunk in group_elements(workflow_parameters, 100):
        promise = []
        for payload in parameter_chunk:
            promise.append(extract.step(telemetry_actor, api, payload).run_async())

        ready, not_ready = ray.wait(promise)
        ray.get(not_ready)

        for obj_ref in promise:
            del obj_ref

        for obj_ref in parameter_chunk:
            del obj_ref


def run(args, key_id, secret_key):
    # ray.init(ignore_reinit_error=True, address='auto', _redis_password='d4t4bricks')
    workflow.init(storage=WORKFLOW_STORAGE)

    telemetry_actor = TelemetryActor.remote()

    start = time.time()

    alpaca.step(args, key_id, secret_key, telemetry_actor).run()

    stop = time.time() - start
    print("Total time: ", round(stop/60, 2))

    df = ray.get(telemetry_actor.collect.remote())
    file_path = STORAGE_DIR / "telemetry"
    if DB_CLUSTER_ID:
        file_path /= DB_CLUSTER_ID

    file_path.mkdir(exist_ok=True, parents=True)
    now = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M")
    file_name = str(file_path / f"{now}.parquet")
    df.to_parquet(file_name, version=ALPACA_PARQUET_VERSION)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        description="Alpaca ray downloader with a rate limit."
    )

    # Symbols
    parser.add_argument("--symbols", type=str, help="Symbols to download data for")

    # Assets
    parser.add_argument("--quotes", action="store_true", help="Download quotes")
    parser.add_argument("--trades", action="store_true", help="Download trades")
    parser.add_argument("--bars", action="store_true", help="Download bars")

    # Dates
    parser.add_argument("--yesterday", action="store_true", help="Download bars")

    parser.add_argument(
        "--start-date",
        type=str,
        dest="start_date",
        help="Begin to download from this date on",
    )

    parser.add_argument(
        "--stop-date", type=str, dest="end_date", help="Stop download from this date on"
    )

    # Rate limit
    parser.add_argument("--limited", action="store_true", help="Use rate limited API key")

    # Formats
    parser.add_argument("--parquet", action="store_true", help="Save in parquet format")

    # default to bars

    args = parser.parse_args()

    if args.symbols:
        args.symbols = args.symbols.split(",")

    if args.start_date:
        try:
            datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
        except ValueError:
            print("Can not parse --start-date")
            sys.exit(1)
    else:
        start_date = datetime.datetime.now() - datetime.timedelta(ALPACA_OFFSET)
        args.start_date = start_date.strftime("%Y-%m-%d")

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
        if args.start_date:
            start_date = datetime.datetime.strptime(args.start_date, "%Y-%m-%d")
            end_date = start_date + datetime.timedelta(2)
            args.end_date = end_date.strftime("%Y-%m-%d")

    print(args.end_date, args.start_date)

    # argparse trades, date, stock

    logging.basicConfig(level=logging.ERROR)

    if DB_CLUSTER_ID:
        if args.limited:
            key_id = dbutils.secrets.get("dbc", "alpaca-key-id")
            secret_key = dbutils.secrets.get("dbc", "alpaca-key-secret")
        else:
            key_id = dbutils.secrets.get("dbc", "alpaca-key-id-unlimited")
            secret_key = dbutils.secrets.get("dbc", "alpaca-key-secret-unlimited")
    else:
        key_id = os.getenv("APCA_API_KEY_ID")
        secret_key = os.getenv("APCA_API_SECRET_KEY")
        if not (key_id and secret_key):
            print(
                "DB_CLUSTER_ID not set. Set APCA_API_KEY_ID and APCA_API_SECRET_KEY to use local secrets."
            )
            sys.exit(1)

    # signal.signal(signal.SIGINT, sigint_handler)
    try:
        run(args, key_id, secret_key)
    except KeyboardInterrupt:
        ray.shutdown()
        sys.exit(1)
