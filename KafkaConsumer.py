from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer('parking-test', value_deserializer=lambda x: loads(x.decode('utf-8')),bootstrap_servers='localhost:9092')
for msg in consumer:
    print(msg)
