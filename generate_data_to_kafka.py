import time

from faker import Faker
from kafka import KafkaProducer
import json

fake = Faker()


def generate_kafka_data() -> dict:

    kafka_data = {}

    kafka_data["name"] = fake.name()
    kafka_data["company"] = fake.company()
    #    kafka_data["msg"]= fake.sentence(),
    kafka_data["remote_ip"] = fake.ipv4_public()
    kafka_data["user_agent"] = fake.user_agent()
    kafka_data["date"] = str(fake.date_between("today", "+8h"))

    return kafka_data


# print(json.dumps(generate_kafka_data()))

def kafka_producer():
    return KafkaProducer(bootstrap_servers=['{your_ip}:9092'])


def stream_data():

    producer = kafka_producer()


    end_time = time.time() + 120
    while True:
        if time.time() > end_time:
            break


        kafka_data = generate_kafka_data()
        producer.send("fake_data", json.dumps(kafka_data).encode('utf-8'))
        time.sleep(5)


if __name__ == "__main__":
    stream_data()