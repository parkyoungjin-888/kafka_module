from confluent_kafka import Producer
from json import dumps


class KafkaProducerControl:
    def __init__(self, server_urls: list[str], topic: str, key: str = None):
        self.topic = topic
        self.key = key

        self.producer = Producer({
            'bootstrap.servers': ','.join(server_urls),
            'enable.idempotence': True,
            'acks': 'all',
            'transactional.id': 'raw-producer-1',
            'linger.ms': 5,
            'compression.type': 'gzip',
        })

        self.producer.init_transactions()

    def send(self, value: dict):
        try:
            self.producer.begin_transaction()

            self.producer.produce(
                topic=self.topic,
                key=self.key.encode('utf-8') if self.key else None,
                value=dumps(value).encode('utf-8')
            )

            self.producer.flush()
            self.producer.commit_transaction()

        except Exception as e:
            print(f"[Kafka Error] Transaction failed: {e}")
            self.producer.abort_transaction()

    def close(self):
        self.producer.flush()

    def __del__(self):
        self.close()


if __name__ == '__main__':
    from datetime import datetime

    _server_urls = ['192.168.0.100:9091', '192.168.0.100:9092', '192.168.0.100:9093']
    kafka_producer = KafkaProducerControl(_server_urls, 'WEBCAM', 'test_key')

    payload = {
        "device_id": "cam2",
        "name": "cam2_250610_180620_637273.jpg",
        "timestamp": 1749546380.637273,
        "width": 1280,
        "height": 720,
        "img": ""
    }
    kafka_producer.send(payload)

print('Done')
