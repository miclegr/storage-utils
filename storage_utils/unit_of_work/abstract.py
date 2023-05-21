from abc import ABC, abstractmethod
from typing import Type
from ..repository.abstract import Repository

class UnitOfWork(ABC):

    repository_factory: Type[Repository]
    repository: Repository

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.rollback()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

