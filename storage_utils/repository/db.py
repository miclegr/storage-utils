from typing import Any, List, Optional, Type
from sqlalchemy.orm import DeclarativeBase, Query, Session
from sqlalchemy.inspection import inspect
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.sql import Select

from ..protocols import Pushable
from .abstract import Repository

class SqlAlchemyRepository(Repository):

    def __init__(self, session: Session):
        self.session = session

    def _pull_scalars_query(self, query: Query|Select, **context) -> List[Any]:
        scalars = self.session.execute(query).scalars()
        ticks = [scalar.to_domain(**context) for scalar in scalars]
        return ticks

    def _get_dialect(self) -> str:
        return self.session.bind.dialect.name
    
    def _get_insert(self):
        dialect = self._get_dialect()
        if dialect == 'sqlite':
            return sqlite_insert
        elif dialect == 'postgresql':
            return postgres_insert
        else:
            raise AttributeError

    @staticmethod
    def _get_primary_and_cols(base: Type[DeclarativeBase]):
        ref = inspect(base)
        primary = [c.name for c in ref.primary_key]
        cols = [c.name for c in ref.columns if c.name not in primary]
        return primary, cols

    @staticmethod
    def _base_to_dict(base: DeclarativeBase, cols: List[str]):
        return {c: getattr(base, c) for c  in cols}
        
    def _push_type(self, data_type: Pushable, domain_items: List[Any], **context):

        for item in domain_items:
            self.session.add(data_type.from_domain(item, **context))

    def _push_type_if_not_exist(self, data_type: Pushable, domain_items: List[Any], **context):

        if len(domain_items) > 0:
            insert = self._get_insert()
            primary, cols = self._get_primary_and_cols(data_type)
            data = [data_type.from_domain(item, **context) for item in domain_items]

            stmt = insert(data_type).values([self._base_to_dict(d, primary+cols) for d in data])
            stmt = stmt.on_conflict_do_nothing(
                    index_elements=primary,
                    )
            self.session.execute(stmt)

    def _upsert_type(self, data_type: Pushable, columns_subset: List[str], domain_items: List[Any], **context):

        if len(domain_items) > 0:
            insert = self._get_insert()
            primary, cols = self._get_primary_and_cols(data_type)
            data = [data_type.from_domain(item, **context) for item in domain_items]

            stmt = insert(data_type).values([self._base_to_dict(d, primary+cols) for d in data])
            stmt = stmt.on_conflict_do_update(
                    index_elements=primary,
                    set_={name: getattr(stmt.excluded, name) for name in columns_subset}
                    )

            self.session.execute(stmt)
