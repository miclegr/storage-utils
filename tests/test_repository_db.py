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
        repository._upsert_type(DataTick, [tick0], update_only)

        session.commit()

    tick1 = DomainTick.from_dict(fake_data[1])
    original_volume = tick1.volume
    original_close = tick1.close

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._upsert_type(DataTick, [tick1], update_only)

        session.commit()

    new_volume = original_volume + 100
    new_close = original_close + 100
    tick1.volume = new_volume
    tick1.close = new_close

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._upsert_type(DataTick, [tick1], update_only)

        session.commit()

    query = select(DataTick).order_by(DataTick.volume)
    with sqlite_session_factory() as session:
        [tick_data0, tick_data1] = session.execute(query).scalars()

    tick1.close = original_close

    assert tick0 == tick_data0.to_domain()
    assert tick1 == tick_data1.to_domain()

def test_push_type_relationship(
    sqlite_session_factory
):

    domain_a = {
        "id": "0",
        "value": "foo",
        "bs": [
            {"id": "0", "value":"foo", "cs": [{"id": "0", "value": "foo"}, {"id": "1", "value": "foo"}]},
            {"id": "1", "value":"foo", "cs": [{"id": "2", "value":"foo"}, {"id": "3", "value":"foo"}]},
        ],
    }

    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._push_type(A, [domain_a], push_relationships=True)
        session.commit()

    with sqlite_session_factory() as session:

        as_ = list(session.execute(select(A)).scalars())
        bs_ = list(session.execute(select(B)).scalars())
        cs_ = list(session.execute(select(C)).scalars())

    assert len(as_) == 1
    assert as_[0].id == domain_a["id"]
    assert as_[0].value == domain_a["value"]

    for b, b_domain in zip(bs_, domain_a["bs"]):
        assert b.id == b_domain["id"]
        assert b.value == b_domain["value"]

    for c, c_domain in zip(cs_, sum([x["cs"] for x in domain_a["bs"]], [])):
        assert c.id == c_domain["id"]
        assert c.value == c_domain["value"]

def test_push_type_if_not_exists_relationship(
    sqlite_session_factory
):

    domain_a = {
        "id": "0",
        "value": "foo",
        "bs": [
            {"id": "0", "value":"foo", "cs": [{"id": "0", "value": "foo"}, {"id": "1", "value": "foo"}]},
            {"id": "1", "value":"foo", "cs": [{"id": "2", "value":"foo"}, {"id": "3", "value":"foo"}]},
        ],
    }

    domain_a1 = {
        "id": "0",
        "value": "bar",
        "bs": [
            {"id": "0", "value":"bar", "cs": [{"id": "0", "value": "bar"}, {"id": "1", "value": "bar"}]},
            {"id": "1", "value":"bar", "cs": [{"id": "2", "value":"bar"}, {"id": "3", "value":"bar"}]},
        ],
    }


    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._push_type_if_not_exist(A, [domain_a], push_relationships=True)
        repository._push_type_if_not_exist(A, [domain_a1], push_relationships=True)
        session.commit()

    with sqlite_session_factory() as session:

        as_ = list(session.execute(select(A)).scalars())
        bs_ = list(session.execute(select(B)).scalars())
        cs_ = list(session.execute(select(C)).scalars())

    assert len(as_) == 1
    assert as_[0].id == domain_a["id"]
    assert as_[0].value == domain_a["value"]

    for b, b_domain in zip(bs_, domain_a["bs"]):
        assert b.id == b_domain["id"]
        assert b.value == b_domain["value"]

    for c, c_domain in zip(cs_, sum([x["cs"] for x in domain_a["bs"]], [])):
        assert c.id == c_domain["id"]
        assert c.value == c_domain["value"]

def test_upsert_type_if_not_exists_relationship(
    sqlite_session_factory
):

    domain_a = {
        "id": "0",
        "value": "foo",
        "bs": [
            {"id": "0", "value":"foo", "cs": [{"id": "0", "value": "foo"}, {"id": "1", "value": "foo"}]},
            {"id": "1", "value":"foo", "cs": [{"id": "2", "value":"foo"}, {"id": "3", "value":"foo"}]},
        ],
    }

    domain_a1 = {
        "id": "0",
        "value": "bar",
        "bs": [
            {"id": "0", "value":"bar", "cs": [{"id": "0", "value": "bar"}, {"id": "1", "value": "bar"}]},
            {"id": "1", "value":"bar", "cs": [{"id": "2", "value":"bar"}, {"id": "3", "value":"bar"}]},
        ],
    }


    with sqlite_session_factory() as session:
        repository = SqlAlchemyRepository(session)
        repository._upsert_type(A, [domain_a], upsert_relationships=True)
        repository._upsert_type(A, [domain_a1], upsert_relationships=True)
        session.commit()

    with sqlite_session_factory() as session:

        as_ = list(session.execute(select(A)).scalars())
        bs_ = list(session.execute(select(B)).scalars())
        cs_ = list(session.execute(select(C)).scalars())

    assert len(as_) == 1
    assert as_[0].id == domain_a1["id"]
    assert as_[0].value == domain_a1["value"]

    for b, b_domain in zip(bs_, domain_a1["bs"]):
        assert b.id == b_domain["id"]
        assert b.value == b_domain["value"]

    for c, c_domain in zip(cs_, sum([x["cs"] for x in domain_a1["bs"]], [])):
        assert c.id == c_domain["id"]
        assert c.value == c_domain["value"]
