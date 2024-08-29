import pytest
from storage_utils.repository.pubsub import PubSubRepository
from storage_utils.retrying import RetryNetworkErrors
from storage_utils.testing.fixtures import *
from storage_utils.unit_of_work.pubsub import PubSubUnitOfWork


@pytest.fixture
def fake_exception():

    class FakeException(Exception):
        pass

    return FakeException

@pytest.fixture
def flaky_pubsub_subscriber_client(fake_pubsub_subscriber_client, fake_exception):


    class FlakyPubsubSubscriberClient(fake_pubsub_subscriber_client):

        errors_pull = 0
        errors_ack = 0

        async def pull(self, *args, **kwargs):
            if self.errors_pull == 0:
                self.errors_pull+=1
                raise fake_exception()
            else:
                self.errors_pull-=1
                return await super().pull(*args, **kwargs)

        async def acknowledge(self, *args, **kwargs):
            if self.errors_ack == 0:
                self.errors_ack+=1
                raise fake_exception()
            else:
                self.errors_pull-=1
                return await super().acknowledge(*args, **kwargs)

    return FlakyPubsubSubscriberClient

@pytest.fixture()
def flaky_pubsub_publisher_client(fake_pubsub_publisher_client, fake_exception):

    class FlakyPubsubPublisherClient(fake_pubsub_publisher_client):
        
        errors = 0

        async def publish(self, *args, **kwargs):
            if self.errors == 0:
                self.errors+=1
                raise fake_exception
            else:
                self.errors-=1
                return await super().publish(*args, **kwargs)
    
    return FlakyPubsubPublisherClient

@pytest.fixture
def fake_retrying_config(fake_exception):
    
    class FakeRetryingConfig(RetryNetworkErrors):
        
        exceptions = (fake_exception,)

    return FakeRetryingConfig

@pytest.fixture
def retriable_repository(fake_retrying_config):

    class RetriablePubSubRepository(PubSubRepository):
        retrying_config = fake_retrying_config

    return RetriablePubSubRepository

@pytest.fixture
def retriable_uow(fake_retrying_config, retriable_repository):

    class RetriablePubSubUnitOfWork(PubSubUnitOfWork):
        retrying_config = fake_retrying_config
        def create_repository(self) -> PubSubRepository:
            self.create_repository_components()
            return retriable_repository(
                self.subscriber_client,
                self.pubsub_config,
                self.ack_buffer,
                self.publisher_buffer,
            )

    return RetriablePubSubUnitOfWork


@pytest.mark.asyncio
async def test_pull_and_ack(
    flaky_pubsub_subscriber_client,
    flaky_pubsub_publisher_client,
    fake_pubsub_subscriber_buffer,
    retriable_uow,
    fake_messages,
    fake_exception
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, flaky_pubsub_subscriber_client, flaky_pubsub_publisher_client
    )
    uow.ack_buffer = fake_pubsub_subscriber_ack_buffer
    fake_pubsub_subscriber_buffer["test"] = fake_messages

    with uow:
        with pytest.raises(fake_exception):
            _ = await uow.repository._pull_from_subscription("test", MessageTick)
        _ = await uow.repository._pull_from_subscription("test", MessageTick)
        with pytest.raises(fake_exception):
            await uow.commit_inbound()
        await uow.commit_inbound()


    uow = retriable_uow(
        pubsub_config, flaky_pubsub_subscriber_client, flaky_pubsub_publisher_client
    )
    uow.ack_buffer = fake_pubsub_subscriber_ack_buffer
    fake_pubsub_subscriber_buffer["test"] = fake_messages

    with uow:
        _ = await uow.repository._pull_from_subscription("test", MessageTick)
        await uow.commit_inbound()

@pytest.mark.asyncio
async def test_publish(
    flaky_pubsub_subscriber_client,
    flaky_pubsub_publisher_client,
    fake_pubsub_publisher_buffer,
    retriable_uow,
    fake_exception,
    fake_data,
):

    pubsub_config = {}
    uow = PubSubUnitOfWork(
        pubsub_config, flaky_pubsub_subscriber_client, flaky_pubsub_publisher_client
    )
    ticks = [DomainTick.from_dict(x) for x in fake_data]

    with uow:
        uow.repository._push_to_topic("test", MessageTick, ticks)
        with pytest.raises(fake_exception):
            await uow.commit_outbound()
        await uow.commit_outbound()
    assert len(fake_pubsub_publisher_buffer["test"])>0

    del fake_pubsub_publisher_buffer["test"]

    uow = retriable_uow(
        pubsub_config, flaky_pubsub_subscriber_client, flaky_pubsub_publisher_client
    )

    with uow:
        uow.repository._push_to_topic("test", MessageTick, ticks)
        await uow.commit_outbound()
    assert len(fake_pubsub_publisher_buffer["test"])>0
