from collections import defaultdict
from datetime import datetime, timedelta
from sqlalchemy import DateTime, Float, String, ForeignKey, event
from sqlalchemy.orm import DeclarativeBase, mapped_column, relationship

from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import json
import uuid

import pytest


class DomainTick:

    ticker: str
    t: datetime
    close: float
    volume: float

    @classmethod
    def from_dict(cls, as_dict):
        new = cls()
        new.ticker = as_dict["ticker"]
        new.t = as_dict["t"]
        new.close = as_dict["close"]
        new.volume = as_dict["volume"]
        if isinstance(new.t, str):
            new.t = datetime.strptime(new.t, "%Y-%m-%d %H:%M:%S")
        return new

    def __eq__(self, other: object) -> bool:
        return self.__dict__ == other.__dict__


class Base(DeclarativeBase):
    pass


class DataTick(Base):

    __tablename__ = "ticks"

    ticker = mapped_column(String, primary_key=True)
    t = mapped_column(DateTime(timezone=True), nullable=False, primary_key=True)
    close = mapped_column(Float, nullable=False)
    volume = mapped_column(Float, nullable=False)

    def to_domain(self):

        dt = DomainTick()
        dt.ticker = self.ticker
        dt.t = self.t
        dt.close = self.close
        dt.volume = self.volume
        return dt

    @classmethod
    def from_domain(cls, domain: DomainTick):

        return cls(
            ticker=domain.ticker, t=domain.t, close=domain.close, volume=domain.volume
        )

    @classmethod
    def from_dict(cls, as_dict):
        new = cls()
        new.ticker = as_dict["ticker"]
        new.t = as_dict["t"]
        new.close = as_dict["close"]
        new.volume = as_dict["volume"]
        return new


class MessageTick:

    ticker: str
    t: datetime
    close: float
    volume: float

    @classmethod
    def from_domain(cls, domain: DomainTick):
        new = cls()
        new.ticker = domain.ticker
        new.t = domain.t
        new.close = domain.close
        new.volume = domain.volume
        return new

    def to_domain(self):
        new = DomainTick()
        new.ticker = self.ticker
        new.t = self.t
        new.close = self.close
        new.volume = self.volume
        return new

    def json(self):
        return json.dumps({**self.__dict__, "t": self.t.strftime("%Y-%m-%d %H:%M:%S")})

    @classmethod
    def parse_raw(cls, raw: str):
        as_dict = json.loads(raw)
        if isinstance(as_dict["t"], str):
            as_dict["t"] = datetime.strptime(as_dict["t"], "%Y-%m-%d %H:%M:%S")
        return cls.from_dict(as_dict)

    @classmethod
    def from_dict(cls, as_dict):
        new = cls()
        new.ticker = as_dict["ticker"]
        new.t = as_dict["t"]
        new.close = as_dict["close"]
        new.volume = as_dict["volume"]
        return new

    def __eq__(self, other: object) -> bool:
        return self.__dict__ == other.__dict__

class A(Base):

    __tablename__ = "a"

    id = mapped_column(String, primary_key=True)
    value = mapped_column(String, primary_key=False)
    bs = relationship("B", back_populates="a")

    @classmethod
    def from_domain(cls, domain_a):
        new = cls()
        new.id = domain_a["id"]
        new.value = domain_a['value']
        new.bs = [B.from_domain(b, new) for b in domain_a["bs"]]
        return new

class B(Base):

    __tablename__ = "b"

    id = mapped_column(String, primary_key=True)
    value = mapped_column(String, primary_key=False)
    id_a = mapped_column(String, ForeignKey(A.id))

    a = relationship("A", back_populates="bs")
    cs = relationship("C", back_populates="b")

    @classmethod
    def from_domain(cls, domain_b, a):
        new = cls()
        new.a = a
        new.id = domain_b["id"]
        new.value = domain_b["value"]
        new.id_a = a.id
        new.cs = [C.from_domain(c, new) for c in domain_b["cs"]]
        return new

class C(Base):

    __tablename__ = "c"

    id = mapped_column(String, primary_key=True)
    value = mapped_column(String, primary_key=False)
    id_b = mapped_column(String, ForeignKey(B.id))

    b = relationship("B", back_populates="cs")

    @classmethod
    def from_domain(cls, domain_c, b):

        new = cls()
        new.id = domain_c["id"]
        new.value = domain_c["value"]
        new.id_b = b.id
        new.b = b
        return new

@pytest.fixture
def base():
    return Base


@pytest.fixture
def in_memory_sqlite_db(base):
    engine = create_engine("sqlite:///:memory:")
    base.metadata.create_all(engine)
    return engine


@pytest.fixture
def on_disk_sqlite_db(base, tmp_path):
    random_name = str(uuid.uuid4())
    engine = create_engine("sqlite:///{}".format(tmp_path / f"{random_name}.db"))
    base.metadata.create_all(engine)

    return engine

@pytest.fixture
def enforce_foreign_key_constraints(on_disk_sqlite_db):

    def _fk_pragma_on_connect(dbapi_con, con_record):
        dbapi_con.execute('pragma foreign_keys=ON;')

    on_disk_sqlite_db.dispose()
    event.listen(on_disk_sqlite_db, 'connect', _fk_pragma_on_connect)

@pytest.fixture
def sqlite_session_factory(on_disk_sqlite_db):
    yield sessionmaker(bind=on_disk_sqlite_db)


@pytest.fixture
def fake_data():

    data = []
    date = datetime(2023, 1, 1, 10, 10, 20)
    for i in range(10):

        data.append(
            {
                "t": date + timedelta(minutes=i),
                "ticker": "SPY",
                "close": 10.0,
                "volume": 20.0,
            }
        )

    return data


@pytest.fixture
def fake_pubsub_publisher_buffer():
    return defaultdict(list)


@pytest.fixture
def fake_pubsub_subscriber_buffer():
    return defaultdict(list)

@pytest.fixture
def fake_pubsub_subscriber_ack_buffer():
    return defaultdict(list)

@pytest.fixture
def fake_messages(fake_data):
    class FakeMessage:
        def __init__(self, ack_id: str, data: bytes) -> None:
            self.ack_id = ack_id
            self.data = data

    messages = []
    for i, tick in enumerate(fake_data):
        message = FakeMessage(
            str(i),
            json.dumps(
                {
                    **tick,
                    "t": tick["t"].strftime("%Y-%m-%d %H:%M:%S"),
                }
            ).encode("utf8"),
        )
        messages.append(message)

    return messages


@pytest.fixture
def fake_pubsub_subscriber_client(fake_pubsub_subscriber_buffer, fake_pubsub_subscriber_ack_buffer):
    class FakePubSubSubcriberClient:
        def __init__(self, *args, **kwargs) -> None:
            self.acknowledged = fake_pubsub_subscriber_ack_buffer

        async def pull(self, subscription, max_messages=20, timeout=10):
            messages = fake_pubsub_subscriber_buffer[subscription]
            to_return = []
            while len(messages) > 0 and len(to_return) < max_messages:
                message = messages[0]
                messages = messages[1:]
                to_return.append(message)
            return to_return

        async def acknowledge(self, subscription, ack_ids, timeout=10):

            for ack_id in ack_ids:
                self.acknowledged[subscription].append(ack_id)

            messages = fake_pubsub_subscriber_buffer[subscription]
            fake_pubsub_subscriber_buffer[subscription] = [x for x in messages if x.ack_id not in ack_ids]

        async def close(self):
            pass

    return FakePubSubSubcriberClient


@pytest.fixture
def fake_pubsub_publisher_client(fake_pubsub_publisher_buffer):
    class FakePubSubPublisherClient:
        def __init__(self, *args, **kwargs) -> None:
            pass

        async def publish(self, topic, messages, timeout=10):
            for message in messages:
                fake_pubsub_publisher_buffer[topic].append(message)

        async def close(self):
            pass

    return FakePubSubPublisherClient


class FakeMessage:

    data: bytes
    ack_id: str


def to_fake_message(data: str, i: int):
    message = FakeMessage()
    message.data = data.encode("utf8")
    message.ack_id = str(i)
    return message
