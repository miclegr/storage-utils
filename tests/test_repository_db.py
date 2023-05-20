import pytest
from sqlalchemy.orm import Query
from storage_utils.testing.fixtures import *
from storage_utils.repository.db import SqlAlchemyRepository
from sqlalchemy.sql import select

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

    update_only = ['volume']

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

