from collections import defaultdict
from .abstract import UnitOfWork
from ..repository.pubsub import PubSubRepository
from gcloud.aio.pubsub import SubscriberClient, PublisherClient
from gcloud.aio.pubsub.utils import PubsubMessage

class PubSubUnitOfWork(UnitOfWork):

    repository: PubSubRepository

    def __init__(self, pubsub_config: object,
                 subscriber_client_factory=None,
                 publisher_client_factory=None) -> None:

        self.pubsub_config = pubsub_config

        if subscriber_client_factory is None:
            subscriber_client_factory = SubscriberClient
        if publisher_client_factory is None:
            publisher_client_factory = PublisherClient

        self.subscriber_client_factory = subscriber_client_factory
        self.publisher_client_factory = publisher_client_factory

        super().__init__()

    def create_repository_components(self):
        self.ack_buffer = defaultdict(list)
        self.publisher_buffer = defaultdict(list)
        self.subscriber_client = self.subscriber_client_factory()
        self.publisher_client = self.publisher_client_factory()

    def create_repository(self) -> PubSubRepository:
        self.create_repository_components()
        return PubSubRepository(
                self.subscriber_client,
                self.pubsub_config,
                self.ack_buffer, 
                self.publisher_buffer
                )

    async def commit(self):

        batch_publish = 800
        for topic, messages in self.publisher_buffer.items():
            if len(messages)>0:
                for i in range(0, len(messages), batch_publish):
                    await self.publisher_client.publish(
                            topic,
                            [PubsubMessage(data=message.json().encode('utf-8')) 
                             for message in messages[i:i+batch_publish]]
                            )
                messages[:] = []

        for topic, ack_ids in self.ack_buffer.items():
            if len(ack_ids)>0:
                await self.subscriber_client.acknowledge(
                        topic,
                        ack_ids
                        )
                ack_ids[:] = []

    def rollback(self):
        for _, ack_ids in self.ack_buffer.items():
            ack_ids[:] = []

        for _, messages in self.publisher_buffer.items():
            messages[:] = []
