from kafka import KafkaConsumer
from json import loads

consumer = KafkaConsumer('parking', value_deserializer=lambda x: loads(x.decode('utf-8')))
for msg in consumer:
    print(msg)
