import random
import time
from multiprocessing.pool import ThreadPool

import yaml
from kafka import KafkaProducer


def read_config():
    with open("config.yaml", "r") as yaml_file:
        return yaml.safe_load(yaml_file)


def send_data_to_topic(data: str):
    producer.send(topic=topic, value=data.encode('UTF-8'))
    time.sleep(random.triangular(0.2, 2.5))


def main():
    global producer
    global topic
    config = read_config()
    server = f"{config['bootstrap_servers']['host']}:{config['bootstrap_servers']['port']}"
    producer = KafkaProducer(bootstrap_servers=server)
    csv_file = 'test.csv'
    topic = 'test'
    stream_number = 1

    try:
        with open(csv_file) as data, ThreadPool(stream_number) as pool:
            pool.map(send_data_to_topic, data)
        producer.flush()

    except Exception:
        print(f"It seems an error occured: {Exception}")

    finally:
        producer.close()


if __name__ == '__main__':
    main()
