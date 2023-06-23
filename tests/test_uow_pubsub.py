import json

from storage_utils.testing.fixtures import *
from storage_utils.unit_of_work.pubsub import PubSubUnitOfWork


@pytest.mark.asyncio
async def test_ack(
    fake_pubsub_subscriber_client,
    fake_pubsub_publisher_client,
    fake_pubsub_subscriber_buffer,
    fake_messages,
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, fake_pubsub_subscriber_client, fake_pubsub_publisher_client
    )
    fake_pubsub_subscriber_buffer["test"] = fake_messages

    with uow:
        ticks = await uow.repository._pull_from_subscription("test", MessageTick)
        await uow.commit()

    assert set(uow.subscriber_client.acknowledged["test"]) == set(
        m.ack_id for m in fake_messages
    )

    with uow:
        ticks = await uow.repository._pull_from_subscription("test", MessageTick)

    assert uow.subscriber_client.acknowledged["test"] == []


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
        await uow.commit()

    message_ticks = [MessageTick.from_domain(x) for x in ticks]
    published_ticks = [
        MessageTick.parse_raw(x.data.decode("utf8"))
        for x in fake_pubsub_publisher_buffer["test"]
    ]
    assert published_ticks == message_ticks
