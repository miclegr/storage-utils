import pytest
from collections import defaultdict
from storage_utils.testing.fixtures import *
from storage_utils.repository.pubsub import PubSubRepository


@pytest.mark.asyncio
async def test_pull(
    fake_pubsub_subscriber_client,
    fake_pubsub_subscriber_buffer,
    fake_messages,
    fake_data,
):

    fake_pubsub_subscriber_buffer["test"] = fake_messages
    repository = PubSubRepository(
        fake_pubsub_subscriber_client(), {}, defaultdict(dict), defaultdict(list)
    )

    messages = await repository._pull_from_subscription("test", MessageTick)
    domain_ticks = [DomainTick.from_dict(x) for x in fake_data]
    assert messages[:20] == domain_ticks[:20]


@pytest.mark.asyncio
async def test_push(fake_pubsub_subscriber_client, fake_data):

    publish_buffer = defaultdict(list)
    repository = PubSubRepository(
        fake_pubsub_subscriber_client(), {}, defaultdict(dict), publish_buffer
    )

    domain_ticks = [DomainTick.from_dict(x) for x in fake_data]
    message_ticks = [MessageTick.from_domain(x) for x in domain_ticks]
    repository._push_to_topic("test", MessageTick, domain_ticks)

    assert publish_buffer["test"] == message_ticks
