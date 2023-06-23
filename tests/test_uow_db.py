from sqlalchemy.orm import Session
from sqlalchemy.sql import text

from storage_utils.testing.fixtures import *
from storage_utils.unit_of_work.db import SqlAlchemyUnitOfWork


def insert_ticker(session: Session, tick: DomainTick):
    session.execute(
        text(
            "INSERT INTO ticks (ticker, t, close, volume) "
            " VALUES (:ticker, :t, :close, :volume)"
        ),
        {
            "ticker": tick.ticker,
            "t": tick.t,
            "close": tick.close,
            "volume": tick.volume,
        },
    )


def get_all_ticks(session):

    return session.execute(text("SELECT * from ticks")).all()


def test_commit(sqlite_session_factory, fake_data):

    uow = SqlAlchemyUnitOfWork(sqlite_session_factory)
    tick = DomainTick.from_dict(fake_data[0])

    with uow:
        insert_ticker(uow.repository.session, tick)
        uow.commit()

    with uow:
        [tick_data] = [
            DomainTick.from_dict(r._mapping)
            for r in get_all_ticks(uow.repository.session)
        ]

    assert tick == tick_data


def test_rollback(sqlite_session_factory, fake_data):

    uow = SqlAlchemyUnitOfWork(sqlite_session_factory)
    tick = DomainTick.from_dict(fake_data[0])

    with uow:
        insert_ticker(uow.repository.session, tick)

    with uow:
        data_ticks = get_all_ticks(uow.repository.session)

    assert len(data_ticks) == 0
