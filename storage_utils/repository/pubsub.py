from typing import List, Dict, Type, Any, Tuple
import json
from .abstract import Repository
from ..protocols import Parsable, Pushable


class PubSubRepository(Repository):
    def __init__(
        self,
        pubsub_subscriber_client,
        config: object,
        pubsub_ack_buffer: Dict[str, Dict[int, str]],
        pubsub_publisher_buffer: Dict[str, List[Tuple[str, Pushable]]],
    ) -> None:

        self.pubsub_subscriber_client = pubsub_subscriber_client
        self.config = config
        self.pubsub_ack_buffer = pubsub_ack_buffer
        self.pubsub_publisher_buffer = pubsub_publisher_buffer
        self._timeout = 120

    async def _pull_from_subscription(
        self, subscription: str, MessageType: Parsable, **context
    ):

        messages = await self.pubsub_subscriber_client.pull(
            subscription, max_messages=1000, timeout=self._timeout
        )
        output = []
        for message_raw in messages:

            message = MessageType.parse_raw(message_raw.data.decode("utf-8"))
            domain_message = message.to_domain(**context)
            self.pubsub_ack_buffer[subscription][id(domain_message)]= message_raw.ack_id
            output.append(domain_message)

        return output

    def _push_to_topic(
        self, topic: str, MessageType: Pushable, items: List[Any], ordering_key='', **context
    ):

        buffer = self.pubsub_publisher_buffer[topic]
        for item in items:
            buffer.append((ordering_key, MessageType.from_domain(item, **context)))
