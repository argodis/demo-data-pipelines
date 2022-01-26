# coding: utf-8

import time
import ray

from time import monotonic
from ray.util.metrics import Counter


@ray.remote
class LeakyBucketActor:
    last_leak: float
    rate: int
    capacity: int
    amount: int

    def __init__(self, capacity=200, rate=3) -> None:
        self.capacity = capacity
        self.rate = rate  # msgs per second
        self.last_leak = monotonic()
        self.amount = 0
        self._counter = Counter(
            "num_calls",
            description="Number of call to actor methods",
            tag_keys=("actor_name",),
        )
        self._counter.set_default_tags({"actor_name": "LeakyBucketActor"})

    async def update(self, name, n) -> None:
        self._counter.inc()
        while True:
            now = monotonic()
            elapsed = now - self.last_leak
            decrement = elapsed * self.rate
            new_capacity = max(int(self.amount - decrement) + 1, 0)
            self.amount = new_capacity

            if self.amount + n > self.capacity:
                time.sleep(1)
            else:
                self.last_leak = now
                self.amount += n
                return
