import json

from storage_utils.testing.fixtures import *
from storage_utils.unit_of_work.pubsub import PubSubUnitOfWork


@pytest.mark.asyncio
async def test_ack(
    fake_pubsub_subscriber_client,
    fake_pubsub_publisher_client,
    fake_pubsub_subscriber_buffer,
    fake_pubsub_subscriber_ack_buffer,
    fake_messages,
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, fake_pubsub_subscriber_client, fake_pubsub_publisher_client
    )
    uow.ack_buffer = fake_pubsub_subscriber_ack_buffer
    fake_pubsub_subscriber_buffer["test"] = fake_messages

    with uow:
        ticks = await uow.repository._pull_from_subscription("test", MessageTick)
        await uow.commit_inbound()

    assert set(uow.subscriber_client.acknowledged["test"]) == set(
        m.ack_id for m in fake_messages
    )

    fake_pubsub_subscriber_buffer["test1"] = fake_messages

    with uow:
        ticks = await uow.repository._pull_from_subscription("test1", MessageTick)

    assert uow.subscriber_client.acknowledged["test1"] == []

@pytest.mark.asyncio
async def test_ack_only_excluding(
    fake_pubsub_subscriber_client,
    fake_pubsub_publisher_client,
    fake_pubsub_subscriber_buffer,
    fake_pubsub_subscriber_ack_buffer,
    fake_messages,
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, fake_pubsub_subscriber_client, fake_pubsub_publisher_client
    )
    uow.ack_buffer = fake_pubsub_subscriber_ack_buffer
    fake_pubsub_subscriber_buffer["test"] = fake_messages

    with uow:
        ticks = await uow.repository._pull_from_subscription("test", MessageTick)
        await uow.commit_inbound(only=ticks[:2])

    assert set(uow.subscriber_client.acknowledged["test"]) == set(
        m.ack_id for m in fake_messages[:2]
    )

    fake_pubsub_subscriber_buffer["test1"] = fake_messages

    with uow:
        ticks = await uow.repository._pull_from_subscription("test1", MessageTick)
        await uow.commit_inbound(excluding=ticks[:2])

    assert set(uow.subscriber_client.acknowledged["test1"]) == set(
        m.ack_id for m in fake_messages[2:]
    )

@pytest.mark.asyncio
async def test_publish(
    fake_pubsub_subscriber_client,
    fake_pubsub_publisher_client,
    fake_pubsub_publisher_buffer,
    fake_data,
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, fake_pubsub_subscriber_client, fake_pubsub_publisher_client
    )
    ticks = [DomainTick.from_dict(x) for x in fake_data]

    with uow:
        uow.repository._push_to_topic("test", MessageTick, ticks)

    assert fake_pubsub_publisher_buffer["test"] == []

    with uow:
        uow.repository._push_to_topic("test", MessageTick, ticks)
        await uow.commit_outbound()

    message_ticks = [MessageTick.from_domain(x) for x in ticks]
    published_ticks = [
        MessageTick.parse_raw(x.data.decode("utf8"))
        for x in fake_pubsub_publisher_buffer["test"]
    ]
    assert published_ticks == message_ticks


@pytest.mark.asyncio
async def test_commit(
    fake_pubsub_subscriber_client,
    fake_pubsub_publisher_client,
    fake_pubsub_subscriber_buffer,
    fake_pubsub_publisher_buffer,
    fake_messages,
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, fake_pubsub_subscriber_client, fake_pubsub_publisher_client
    )
    fake_pubsub_subscriber_buffer["test"] = fake_messages

    with uow:
        ticks = await uow.repository._pull_from_subscription("test", MessageTick)
        uow.repository._push_to_topic("test", MessageTick, ticks)
        await uow.commit()

    assert set(uow.subscriber_client.acknowledged["test"]) == set(
        m.ack_id for m in fake_messages
    )

    message_ticks = [MessageTick.from_domain(x) for x in ticks]
    published_ticks = [
        MessageTick.parse_raw(x.data.decode("utf8"))
        for x in fake_pubsub_publisher_buffer["test"]
    ]
    assert published_ticks == message_ticks

