from typing import List, Any
from collections import defaultdict
from .abstract import UnitOfWork
from ..repository.pubsub import PubSubRepository
from gcloud.aio.pubsub import SubscriberClient, PublisherClient
from gcloud.aio.pubsub.utils import PubsubMessage
from gcloud.aio.auth.token import Token

TOKEN = Token(scopes=[
    'https://www.googleapis.com/auth/pubsub',
    ])


class PubSubUnitOfWork(UnitOfWork):

    repository: PubSubRepository

    def __init__(
        self,
        pubsub_config: object,
        subscriber_client_factory=None,
        publisher_client_factory=None,
    ) -> None:

        self.pubsub_config = pubsub_config
        self._timeout = 30
        self._constant_ordering_key = 'key'

        if subscriber_client_factory is None:
            subscriber_client_factory = lambda : SubscriberClient(token=TOKEN)
        if publisher_client_factory is None:
            publisher_client_factory = lambda : PublisherClient(token=TOKEN)

        self.subscriber_client_factory = subscriber_client_factory
        self.publisher_client_factory = publisher_client_factory

        super().__init__()

    def create_repository_components(self):
        self.ack_buffer = defaultdict(dict)
        self.publisher_buffer = defaultdict(list)
        self.subscriber_client = self.subscriber_client_factory()
        self.publisher_client = self.publisher_client_factory()

    def create_repository(self) -> PubSubRepository:
        self.create_repository_components()
        return PubSubRepository(
            self.subscriber_client,
            self.pubsub_config,
            self.ack_buffer,
            self.publisher_buffer,
        )

    async def commit_outbound(self):

        batch_publish = 800
        for topic, messages in self.publisher_buffer.items():
            if len(messages) > 0:
                for i in range(0, len(messages), batch_publish):
                    await self.publisher_client.publish(
                        topic,
                        [
                            PubsubMessage(
                                data=message.json().encode("utf-8"),
                                ordering_key=self._constant_ordering_key
                                )
                            for message in messages[i : i + batch_publish]
                        ],
                        timeout=self._timeout,
                    )
                messages[:] = []

    async def commit_inbound(self, 
                             only: List[Any] | None = None, 
                             excluding: List[Any] | None = None):
        if only is not None:
            only_ids = set(id(x) for x in only)
        elif excluding is not None:
            excluding_ids = set(id(x) for x in excluding)

        for topic, ack_ids in self.ack_buffer.items():
            if len(ack_ids) > 0:

                if only is not None:
                    ack_ids_filtered = [ack_id for x, ack_id in ack_ids.items() if x in only_ids]
                elif excluding is not None:
                    ack_ids_filtered = [ack_id for x, ack_id in ack_ids.items() if x not in excluding_ids]
                else:
                    ack_ids_filtered = list(ack_ids.values())

                await self.subscriber_client.acknowledge(
                    topic, ack_ids_filtered, timeout=self._timeout
                )
                ack_ids.clear() 

    async def close_clients(self):
        await self.publisher_client.close()
        await self.subscriber_client.close()

    async def commit(self):

        await self.commit_outbound()
        await self.commit_inbound()
        
        await self.close_clients()


    def rollback(self):
        for _, ack_ids in self.ack_buffer.items():
            ack_ids.clear()

        for _, messages in self.publisher_buffer.items():
            messages[:] = []
