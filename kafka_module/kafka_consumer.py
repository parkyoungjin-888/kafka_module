from confluent_kafka import Consumer, KafkaError
import json
from typing import Callable


class KafkaConsumerControl:
    def __init__(self, host: str, port: int, topic: str,
                 auto_offset_reset: str = 'earliest', group_id: str = 'default-group'):
        self.topic = topic
        self.consumer = Consumer({
            'bootstrap.servers': f'{host}:{port}',
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': True,
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
                    data = json.loads(value.decode('utf-8'))
                    call_back(data)

        except KeyboardInterrupt:
            print("Kafka consumer interrupted")

        finally:
            self.close()

    def close(self):
        self.consumer.close()

    def __del__(self):
        self.close()


if __name__ == '__main__':
    def process_message(data):
        print("Received:", data)

    consumer = KafkaConsumerControl(
        host='192.168.0.100',
        port=9091,
        topic='test_topic',
        group_id='test_group'
    )
    consumer.start_consumer(process_message)

