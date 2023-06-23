from abc import ABC, abstractmethod
from ..repository.abstract import Repository


class UnitOfWork(ABC):

    repository: Repository

    @abstractmethod
    def create_repository(self) -> Repository:
        raise NotImplementedError

    def __enter__(self):
        self.repository = self.create_repository()
        return self

    def __exit__(self, *args):
        self.rollback()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError
