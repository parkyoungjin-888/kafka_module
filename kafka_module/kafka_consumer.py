from kafka import KafkaConsumer
import json
from typing import Callable


class KafkaConsumerControl:
    def __init__(self, host: str, port: int, topic: str,
                 auto_offset_reset: str = 'earliest', enable_auto_commit: bool = True, group_id=None):
        self.consumer = KafkaConsumer(
            topic,
            bootstrap_servers=[f'{host}:{port}'],
            auto_offset_reset=auto_offset_reset,
            enable_auto_commit=enable_auto_commit,
            group_id=group_id,
            key_deserializer=lambda k: k.decode('utf-8'),
            value_deserializer=lambda x: json.loads(x.decode('utf-8'))
        )

    def start_consumer(self, call_back: Callable[[dict], None]):
        for message in self.consumer:
            call_back(message.value)

    def close(self):
        self.consumer.close()

    def __del__(self):
        self.close()
