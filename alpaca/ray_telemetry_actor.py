# coding: utf-8

import ray
import time
import pandas as pd

from ray.util.metrics import Counter


@ray.remote
class TelemetryActor:
    """
    Collects telemetry data from requests to Alpaca API.
    """

    def __init__(self) -> None:
        self._data = []

        self._counter = Counter(
            "num_calls", description="Number of call to actor", tag_keys=("actor_name",)
        )
        self._counter.set_default_tags({"actor_name": "TelemetryActor"})

    async def add(self, day: str, asset: str, duration: float) -> None:
        self._counter.inc()
        self._data.append((day, asset, duration))

    async def collect(self) -> pd.DataFrame:
        return pd.DataFrame(data=self._data, columns=["day", "asset", "duration"])
