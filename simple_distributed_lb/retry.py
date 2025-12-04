from typing import Callable


class RetryMechanism:
    """
    Iterable retry mechanism with exponential backoff.

    Supports both limited and infinite retry attempts for different use cases:
    - Limited retries (max_retries > 0): Suitable for transient errors
    - Infinite retries (max_retries = float('inf')): Suitable for persistent reconnection
    """

    def __init__(
        self,
        max_retries: float = 3,
        min_wait: float = 0.1,
        max_wait: float = 3,
        retry_fn: Callable[[int], float] = None,
    ):
        self.max_retries = max_retries
        self.min_wait = min_wait
        self.max_wait = max_wait
        self.retry_fn = retry_fn
        if retry_fn is None:
            self.retry_fn = lambda attempt: min(max(self.min_wait * (2**attempt), self.min_wait), self.max_wait)

    def __iter__(self):
        self.attempt = 0
        return self

    def __next__(self):
        self.attempt += 1
        if self.attempt > self.max_retries:
            raise StopIteration
        return self.retry_fn(self.attempt)

    def reset(self):
        self.attempt = 0
