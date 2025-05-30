from confluent_kafka import Consumer, KafkaError
import json
from typing import Callable


class KafkaConsumerControl:
    def __init__(self, server_urls: list[str], topic: str,
                 auto_offset_reset: str = 'earliest', group_id: str = 'default-group',
                 enable_auto_commit: bool = False):
        self.topic = topic
        self.enable_auto_commit = enable_auto_commit

        self.consumer = Consumer({
            'bootstrap.servers': ','.join(server_urls),
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
        })

        self.consumer.subscribe([self.topic])

    def start_consumer(self, call_back: Callable[[dict], None]):
        try:
            while True:
                msg = self.consumer.poll(1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        print(f"[Kafka Error] {msg.error()}")
                    continue

                value = msg.value()
                if value is not None:
                    try:
                        data = json.loads(value.decode('utf-8'))
                        call_back(data)

                        if not self.enable_auto_commit:
                            self.consumer.commit(msg)
                    except Exception as e:
                        print(f"[Processing Error] {e}")

        except KeyboardInterrupt:
            print("Kafka consumer interrupted")

        finally:
            self.close()

    def close(self):
        if hasattr(self, 'consumer'):
            self.consumer.close()

    def __del__(self):
        self.close()


if __name__ == '__main__':
    def process_message(data):
        print("Received:", data)

    consumer = KafkaConsumerControl(
        server_urls=['192.168.0.100:9091'],
        topic='test_topic',
        group_id='test_group'
    )
    consumer.start_consumer(process_message)

