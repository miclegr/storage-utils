from collections import defaultdict
from typing import Any, Callable, List, Optional, Type, Dict
from sqlalchemy.orm import DeclarativeBase, Query, Session
from sqlalchemy.inspection import inspect
from sqlalchemy.orm import RelationshipDirection
from sqlalchemy.dialects.sqlite import insert as sqlite_insert
from sqlalchemy.dialects.postgresql import insert as postgres_insert
from sqlalchemy.sql import Select

from ..protocols import Pushable
from .abstract import Repository


class SqlAlchemyRepository(Repository):
    
    _relationship_topological_order = {}

    def __init__(self, session: Session):
        self.session = session

    def _pull_scalars_query(self, query: Query | Select, **context) -> List[Any]:
        scalars = self.session.execute(query).scalars()
        ticks = [scalar.to_domain(**context) for scalar in scalars]
        return ticks

    def _get_dialect(self) -> str:
        return self.session.bind.dialect.name

    def _get_insert(self):
        dialect = self._get_dialect()
        if dialect == "sqlite":
            return sqlite_insert
        elif dialect == "postgresql":
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
    def _get_relationships(base: Type[DeclarativeBase]):
        ref = inspect(base)
        return ref.relationships

    @staticmethod
    def _base_to_dict(base: DeclarativeBase, cols: List[str]):
        return {c: getattr(base, c) for c in cols}

    def _push_also_relationships(
            self, data: List[Pushable], data_type: Pushable, handle_conflict: str, statement_buffer: Dict
    ):

        insert = self._get_insert()
        stack = [(data, data_type, (data_type,))]
        while len(stack):
            data, data_type, exclude = stack.pop()
            relationships = [
                x
                for x in self._get_relationships(data_type)
                if x.mapper.class_ not in exclude
            ]
            for relationship in relationships:

                new_data = [
                    getattr(item, relationship.key)
                    for item in data
                    ]
                if relationship.uselist:
                    new_data = sum(new_data,[])

                new_data_type = relationship.mapper.class_
                primary, cols = self._get_primary_and_cols(new_data_type)

                if len(new_data) > 0:
                    
                    stmt = insert(new_data_type).values(
                        [self._base_to_dict(d, primary + cols) for d in new_data]
                    )
                    if handle_conflict == 'dont':
                        pass
                    elif handle_conflict == 'on_conflict_do_nothing':
                        stmt = stmt.on_conflict_do_nothing(
                            index_elements=primary,
                        )
                    elif handle_conflict == 'on_conflict_do_update':
                        if len(cols)>0:
                            stmt = stmt.on_conflict_do_update(
                                index_elements=primary,
                                set_={name: getattr(stmt.excluded, name) for name in cols},
                            )
                        else:
                            stmt = stmt.on_conflict_do_nothing(
                                index_elements=primary,
                            )
                    else:
                        raise NotImplementedError

                    statement_buffer[new_data_type].append(stmt)

                stack.append((new_data, new_data_type, (*exclude, new_data_type)))

    def _sort_relationship_topologically(self, root_data_type: Pushable) -> List[Type[Pushable]]:
        
        if root_data_type in self._relationship_topological_order:
            return self._relationship_topological_order[root_data_type]

        relationships = set()
        seen = set([root_data_type])
        stack = [root_data_type]
        while len(stack):
            data_type = stack.pop()
            ref = inspect(data_type)
            depends = []
            for k in ref.relationships:
                related_with = k.mapper.class_
                if k.direction == RelationshipDirection.MANYTOONE:
                    assert len(k.remote_side) == 1
                    depends.append(related_with)
                elif k.direction == RelationshipDirection.MANYTOMANY:
                    raise NotImplementedError
                
                if related_with not in seen:
                    stack.append(related_with)
                    seen.add(related_with)
            relationships.add((data_type, tuple(depends)))

        to_sort_topologically = sorted(map(lambda x: (x[0], list(x[1])),relationships), key=lambda x: -len(x[1]))

        sorted_topologically = []
        while len(to_sort_topologically):
            data_type, depends = to_sort_topologically.pop()

            if len(depends) != 0:
                to_sort_topologically.insert(0, (data_type, depends))
                continue

            for _, other_deps in to_sort_topologically:
                if data_type in other_deps:
                    other_deps.remove(data_type)
            sorted_topologically.append(data_type)

        self._relationship_topological_order[root_data_type] = sorted_topologically
        return sorted_topologically

    def _push_type(
        self,
        data_type: Pushable,
        domain_items: List[Any],
        push_relationships=False,
        **context
    ):

        insert = self._get_insert()
        primary, cols = self._get_primary_and_cols(data_type)
        data = [data_type.from_domain(item, **context) for item in domain_items]

        stmt = insert(data_type).values(
            [self._base_to_dict(d, primary + cols) for d in data]
        )

        if not push_relationships:
            self.session.execute(stmt)

        else :
            statement_buffer = defaultdict(list)
            statement_buffer[data_type].append(stmt)
            self._push_also_relationships(data, data_type, 'dont', statement_buffer)

            order = self._sort_relationship_topologically(data_type)

            for ordered_data_type in order:
                for stmt in statement_buffer[ordered_data_type]:
                    self.session.execute(stmt)

    def _push_type_if_not_exist(
        self,
        data_type: Pushable,
        domain_items: List[Any],
        push_relationships=False,
        **context
    ):

        if len(domain_items) > 0:
            insert = self._get_insert()
            primary, cols = self._get_primary_and_cols(data_type)
            data = [data_type.from_domain(item, **context) for item in domain_items]

            stmt = insert(data_type).values(
                [self._base_to_dict(d, primary + cols) for d in data]
            )
            stmt = stmt.on_conflict_do_nothing(
                index_elements=primary,
            )

            if not push_relationships:
                self.session.execute(stmt)

            else :
                statement_buffer = defaultdict(list)
                statement_buffer[data_type].append(stmt)
                self._push_also_relationships(data, data_type, 'on_conflict_do_nothing', statement_buffer)

                order = self._sort_relationship_topologically(data_type)

                for ordered_data_type in order:
                    for stmt in statement_buffer[ordered_data_type]:
                        self.session.execute(stmt)

    def _upsert_type(
        self,
        data_type: Pushable,
        domain_items: List[Any],
        columns_subset: Optional[List[str]] = None,
        upsert_relationships=False,
        **context
    ):

        if len(domain_items) > 0:
            insert = self._get_insert()
            primary, cols = self._get_primary_and_cols(data_type)
            if columns_subset is None:
                columns_subset = cols

            data = [data_type.from_domain(item, **context) for item in domain_items]

            stmt = insert(data_type).values(
                [self._base_to_dict(d, primary + cols) for d in data]
            )
            stmt = stmt.on_conflict_do_update(
                index_elements=primary,
                set_={name: getattr(stmt.excluded, name) for name in columns_subset},
            )

            if not upsert_relationships:
                self.session.execute(stmt)

            else :
                statement_buffer = defaultdict(list)
                statement_buffer[data_type].append(stmt)
                self._push_also_relationships(data, data_type, 'on_conflict_do_update', statement_buffer)

                order = self._sort_relationship_topologically(data_type)

                for ordered_data_type in order:
                    for stmt in statement_buffer[ordered_data_type]:
                        self.session.execute(stmt)
