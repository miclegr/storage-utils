from typing import List, Dict, Type, Any
import json
from .abstract import Repository
from ..protocols import Parsable, Pushable


class PubSubRepository(Repository):

    def __init__(self, 
                 pubsub_subscriber_client,
                 config: object,
                 pubsub_ack_buffer: Dict[str,List[str]],
                 pubsub_publisher_buffer: Dict[str, List[Pushable]]) -> None:

        self.pubsub_subscriber_client = pubsub_subscriber_client
        self.config = config
        self.pubsub_ack_buffer = pubsub_ack_buffer
        self.pubsub_publisher_buffer = pubsub_publisher_buffer

    async def _pull_from_subscription(self, subscription: str, MessageType: Parsable, **context):

        messages = await self.pubsub_subscriber_client.pull(subscription, max_messages=20)
        output = []
        for message in messages:

            self.pubsub_ack_buffer[subscription].append(message.ack_id)
            message = MessageType.parse_raw(message.data.decode('utf-8'))
            output.append(message.to_domain(**context))

        return output

    def _push_to_topic(self, topic:str, MessageType: Pushable, items: List[Any], **context):

        buffer = self.pubsub_publisher_buffer[topic]
        for item in items:
            buffer.append(MessageType.from_domain(item, **context))
