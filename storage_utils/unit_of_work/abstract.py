from abc import ABC, abstractmethod
from ..repository.abstract import Repository

class UnitOfWork(ABC):

    repository: Repository

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.rollback()

    @abstractmethod
    def rollback(self):
        raise NotImplementedError

