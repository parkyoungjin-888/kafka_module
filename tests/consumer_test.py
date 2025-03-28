import cv2
from datetime import datetime

from kafka_module.kafka_consumer import KafkaConsumerControl
from data_model_module.raw_data_model import Imgdata


class TestControl:
    def __init__(self):
        self.kafka_consumer = KafkaConsumerControl('192.168.0.104', 9092, 'test_topic')
        self.temp_queue = []

    def test_call_back_func(self, data: dict):
        dict_data = Imgdata(**data).get_dict_with_img_decoding()

        # self.temp_queue.append(dict_data)
        # print(self.temp_queue)

        consumed_timestamp = datetime.now().timestamp()
        time_offset = consumed_timestamp - dict_data['timestamp']

        img = dict_data['img']
        cv2.putText(img, f'{time_offset} sec', (20, 50), cv2.FONT_HERSHEY_PLAIN, 1, (0, 0, 255), 1, cv2.LINE_AA)

        cv2.imshow('img', img)
        cv2.waitKey(10)

    def run(self):
        self.kafka_consumer.start_consumer(self.test_call_back_func)

    def close(self):
        self.kafka_consumer.close()


if __name__ == '__main__':
    test_control = TestControl()
    test_control.run()
    test_control.close()
