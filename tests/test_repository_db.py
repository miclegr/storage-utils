import pytest
from sqlalchemy.orm import Query, relationship, mapped_column
from storage_utils.testing.fixtures import *
from storage_utils.repository.db import SqlAlchemyRepository
from sqlalchemy.sql import select
from sqlalchemy import ForeignKey, String


def test_pull_scalars_query(sqlite_session_factory, fake_data):

    tick = DomainTick.from_dict(fake_data[0])
    data_tick = DataTick.from_domain(tick)

    with sqlite_session_factory() as session:
        session.add(data_tick)
        session.commit()

    with sqlite_session_factory() as session:
        query = select(DataTick)
        repository = SqlAlchemyRepository(session)

        data_ticks = repository._pull_scalars_query(query)

    assert data_ticks == [tick]


def test_push_type(sqlite_session_factory, fake_data):

    tick = DomainTick.from_dict(fake_data[0])

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._push_type(DataTick, [tick])

        session.commit()

    query = select(DataTick)
    with sqlite_session_factory() as session:
        [tick_data] = session.execute(query).scalars()

    assert tick == tick_data.to_domain()


def test_push_type_if_not_exists(sqlite_session_factory, fake_data):

    tick = DomainTick.from_dict(fake_data[0])

    original_volume = tick.volume

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._push_type(DataTick, [tick])

        session.commit()

    new_volume = original_volume + 100
    tick.volume = new_volume

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._push_type_if_not_exist(DataTick, [tick])

        session.commit()

    query = select(DataTick)
    with sqlite_session_factory() as session:
        [tick_data] = session.execute(query).scalars()

    assert tick_data.volume == original_volume


def test_upsert_type(sqlite_session_factory, fake_data):

    update_only = ["volume"]

    tick0 = DomainTick.from_dict(fake_data[0])

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._upsert_type(DataTick, update_only, [tick0])

        session.commit()

    tick1 = DomainTick.from_dict(fake_data[1])
    original_volume = tick1.volume
    original_close = tick1.close

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._upsert_type(DataTick, update_only, [tick1])

        session.commit()

    new_volume = original_volume + 100
    new_close = original_close + 100
    tick1.volume = new_volume
    tick1.close = new_close

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._upsert_type(DataTick, update_only, [tick1])

        session.commit()

    query = select(DataTick).order_by(DataTick.volume)
    with sqlite_session_factory() as session:
        [tick_data0, tick_data1] = session.execute(query).scalars()

    tick1.close = original_close

    assert tick0 == tick_data0.to_domain()
    assert tick1 == tick_data1.to_domain()


def test_push_type_if_not_exists_relationship(
    base, on_disk_sqlite_db, sqlite_session_factory
):
    class A(base):

        __tablename__ = "a"

        id = mapped_column(String, primary_key=True)
        bs = relationship("B", back_populates="a")

        @classmethod
        def from_domain(cls, domain_a):
            new = cls()
            new.id = domain_a["id"]
            new.bs = [B.from_domain(b, new) for b in domain_a["bs"]]
            return new

    class B(base):

        __tablename__ = "b"

        id = mapped_column(String, primary_key=True)
        id_a = mapped_column(String, ForeignKey(A.id))

        a = relationship("A", back_populates="bs")
        cs = relationship("C", back_populates="b")

        @classmethod
        def from_domain(cls, domain_b, a):
            new = cls()
            new.a = a
            new.id = domain_b["id"]
            new.id_a = a.id
            new.cs = [C.from_domain(c, new) for c in domain_b["cs"]]
            return new

    class C(base):

        __tablename__ = "c"

        id = mapped_column(String, primary_key=True)
        id_b = mapped_column(String, ForeignKey(B.id))

        b = relationship("B", back_populates="cs")

        @classmethod
        def from_domain(cls, domain_c, b):

            new = cls()
            new.id = domain_c["id"]
            new.id_b = b.id
            new.b = b
            return new

    base.metadata.create_all(on_disk_sqlite_db)

    domain_a = {
        "id": "0",
        "bs": [
            {"id": "0", "cs": [{"id": "0"}, {"id": "1"}]},
            {"id": "1", "cs": [{"id": "2"}, {"id": "3"}]},
        ],
    }

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._push_type_if_not_exist(A, [domain_a], push_relationships=True)
        session.commit()

    with sqlite_session_factory() as session:

        as_ = list(session.execute(select(A)).scalars())
        bs_ = list(session.execute(select(B)).scalars())
        cs_ = list(session.execute(select(C)).scalars())

    assert len(as_) == 1
    assert as_[0].id == domain_a["id"]

    for b, b_domain in zip(bs_, domain_a["bs"]):
        assert b.id == b_domain["id"]

    for c, c_domain in zip(cs_, sum([x["cs"] for x in domain_a["bs"]], [])):
        assert c.id == c_domain["id"]
