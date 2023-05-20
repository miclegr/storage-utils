from typing import Callable
from collections import defaultdict
from .abstract import UnitOfWork
from ..repository.pubsub import PubSubRepository
from gcloud.aio.pubsub import SubscriberClient, PublisherClient
from gcloud.aio.pubsub.utils import PubsubMessage

class PubSubUnitOfWork(UnitOfWork):

    repository: PubSubRepository

    def __init__(self, pubsub_config: object,
                 subscriber_client_factory=SubscriberClient,
                 publisher_client_factory=PublisherClient) -> None:

        self.pubsub_config = pubsub_config
        self.subscriber_client_factory = subscriber_client_factory
        self.publisher_client_factory = publisher_client_factory

        super().__init__()

    def __enter__(self):
        self.ack_buffer = defaultdict(list)
        self.publisher_buffer = defaultdict(list)
        self.subscriber_client = self.subscriber_client_factory()
        self.publisher_client = self.publisher_client_factory()
        self.repository = PubSubRepository(
                self.subscriber_client,
                self.pubsub_config,
                self.ack_buffer, 
                self.publisher_buffer
                )

        return super().__enter__()

    async def commit(self):
        
        for topic, messages in self.publisher_buffer.items():
            if len(messages)>0:
                await self.publisher_client.publish(
                        topic,
                        [PubsubMessage(data=message.json().encode('utf-8')) 
                         for message in messages]
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
