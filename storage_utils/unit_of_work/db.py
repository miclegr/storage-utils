from typing import Callable
from .abstract import UnitOfWork
from ..repository.db import SqlAlchemyRepository

class SqlAlchemyUnitOfWork(UnitOfWork):

    repository: SqlAlchemyRepository

    def __init__(self, session_factory: Callable) -> None:

        self.session_factory = session_factory
        super().__init__()

    def __enter__(self):
        self.repository = SqlAlchemyRepository(self.session_factory())
        return super().__enter__()

    def commit(self):
        self.repository.session.commit()

    def rollback(self):
        self.repository.session.rollback()
