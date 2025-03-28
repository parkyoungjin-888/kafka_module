from kafka import KafkaProducer
from json import dumps
from data_model_module.raw_data_model import Rawdata, Imgdata
from data_model_module.validate_decorator import validate_input


class KafkaProducerControl:
    def __init__(self, server_urls: list[str], topic: str, key: str = None):
        self.topic = topic
        self.key = key
        self.producer = KafkaProducer(
            acks=0,
            compression_type='gzip',
            bootstrap_servers=server_urls,
            key_serializer=lambda k: k.encode('utf-8'),
            value_serializer=lambda x: dumps(x).encode('utf-8')
        )

    @validate_input(Rawdata)
    def send_data(self, data_model: Rawdata):
        future = self.producer.send(self.topic, value=data_model.model_dump(), key=self.key)
        result = future.get(timeout=10)

    @validate_input(Imgdata)
    def send_img(self, data_model: Imgdata):
        future = self.producer.send(self.topic, value=data_model.model_dump(), key=self.key)
        result = future.get(timeout=10)

    def close(self):
        self.producer.flush()
        self.producer.close()

    def __del__(self):
        self.close()


if __name__ == '__main__':
    import cv2
    from datetime import datetime

    _server_urls = ['192.168.0.104:9091', '192.168.0.104:9092', '192.168.0.104:9093']

    kafka_producer = KafkaProducerControl(_server_urls, 'test_topic', 'test_img')

    cap = cv2.VideoCapture(0)
    cap.set(cv2.CAP_PROP_FRAME_WIDTH, 1920)
    cap.set(cv2.CAP_PROP_FRAME_HEIGHT, 1080)
    cap.set(cv2.CAP_PROP_FPS, 60)

    _key = 'test'

    for i in range(100):
        ret, img = cap.read()
        _timestamp = datetime.now().timestamp()

        # cv2.imshow('img', img)
        # cv2.waitKey(0)

        _img_data = {
            'name': 'test_img.jpg',
            'timestamp': _timestamp,
            'width': img.shape[1],
            'height': img.shape[0],
            'img': img
        }
        kafka_producer.send_img(**_img_data)

        # _data = Rawdata(timestamp=1725188400, io_id='aaa', value=1.1)
        # kafka_producer.send_data(_data)

    print('end')
