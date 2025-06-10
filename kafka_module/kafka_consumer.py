import os
import asyncio
from confluent_kafka import Consumer, KafkaError
import json
from typing import Callable
from config_module.config_singleton import ConfigSingleton
from utils_module.logger import LoggerSingleton


class KafkaConsumerControl:
    def __init__(self, server_urls: list[str], topic: str,
                 auto_offset_reset: str = 'earliest', group_id: str = 'default-group',
                 enable_auto_commit: bool = False):
        config = ConfigSingleton()
        app_config = config.get_value('app')
        log_level = os.environ.get('LOG_LEVEL', 'DEBUG')
        self.logger = LoggerSingleton.get_logger(f'{app_config["name"]}.kafka', level=log_level)

        self.topic = topic
        self.enable_auto_commit = enable_auto_commit

        self.consumer = Consumer({
            'bootstrap.servers': ','.join(server_urls),
            'group.id': group_id,
            'auto.offset.reset': auto_offset_reset,
            'enable.auto.commit': enable_auto_commit,
        })

        self.consumer.subscribe([self.topic])

    async def start_consumer_async(self, call_back: Callable[[dict], None]):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    await asyncio.sleep(0.01)
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        self.logger.error(msg.error())
                    continue

                value = msg.value()
                if value is not None:
                    try:
                        data = json.loads(value.decode('utf-8'))
                        await call_back(data)
                        if not self.enable_auto_commit:
                            self.consumer.commit(msg)
                    except Exception as e:
                        self.logger.error(f"Consumer Error: {str(e)}")
        except KeyboardInterrupt:
            print("Kafka consumer interrupted")
        finally:
            self.close()

    def start_consumer_sync(self, call_back: Callable[[dict], None]):
        try:
            while True:
                msg = self.consumer.poll(1.0)
                if msg is None:
                    continue
                if msg.error():
                    if msg.error().code() != KafkaError._PARTITION_EOF:
                        self.logger.error(msg.error())
                    continue

                value = msg.value()
                if value is not None:
                    try:
                        data = json.loads(value.decode('utf-8'))
                        call_back(data)
                        if not self.enable_auto_commit:
                            self.consumer.commit(msg)
                    except Exception as e:
                        self.logger.error(f"Consumer Error: {str(e)}")
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
    consumer.start_consumer_sync(process_message)

