from abc import ABC, abstractmethod
import aiohttp
from functools import reduce
import operator
from tenacity import retry, retry_if_exception, stop_after_attempt, wait_exponential
from typing import Callable, Iterable, Type


class RetryingConfig(ABC):

    @classmethod
    @abstractmethod
    def to_decorator(cls) -> Callable:
        pass

class RetryNetworkErrors(RetryingConfig):

    exceptions: Iterable[Type[Exception]] = (aiohttp.ClientResponseError,)
    exponential_rate: int = 1
    exponential_max: int = 10
    stop_after_attempt: int = 5

    @classmethod
    def to_decorator(cls) -> Callable: 
        retry_if = reduce(operator.or_, map(retry_if_exception,cls.exceptions))
        return retry(
                wait=wait_exponential(multiplier=cls.exponential_rate, max=cls.exponential_max),
                retry=retry_if,
                stop=stop_after_attempt(cls.stop_after_attempt)
                )

class NoRetry(RetryingConfig):

    @classmethod
    def to_decorator(cls) -> Callable:
        def _inner_f(func):
            return func
        return _inner_f


