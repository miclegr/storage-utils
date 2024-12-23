from abc import ABC, abstractmethod
import aiohttp
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_exponential
from typing import Callable, Tuple, Type


class RetryingConfig(ABC):

    @classmethod
    @abstractmethod
    def to_decorator(cls) -> Callable:
        pass

class RetryNetworkErrors(RetryingConfig):

    exceptions: Tuple[Type[Exception], ...] = (aiohttp.ClientResponseError,aiohttp.ClientOSError)
    exponential_rate: int = 1
    exponential_max: int = 10
    stop_after_attempt: int = 5

    @classmethod
    def to_decorator(cls) -> Callable: 
        retry_if = retry_if_exception_type(cls.exceptions)
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


