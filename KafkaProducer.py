import csv
from json import dumps
from time import sleep

from kafka import KafkaProducer

# bootstrap_servers=['localhost:9092']:sets the host and port the producer should.
# contact to bootstrap initial cluster metadata.
# It is not necessary to set this here, since the default is localhost:9092.

# value_serializer=lambda x: json.dumps(x).encode(‘utf-8’): function
# of how the data should be serialized before sending to the broker.
# Here, we convert the data to a json file and encode it to utf-8.
producer = KafkaProducer(bootstrap_servers='localhost:9092', value_serializer=lambda x: dumps(x).encode('utf-8'))

# we want to generate numbers from one till 1000.
# This can be done with a for-loop where we feed each number as the value into a dictionary with one key: number.
# for e in range(1000):
#   data = {'number': e}
#    producer.send('parking', value=data, key="")
#    sleep(5)

with open('dataset/aarhus_parking.csv') as csvFile:
    csvReader = csv.reader(csvFile, delimiter=',')
    firstLine = True
    line_count = 0
    for line in csvReader:
        # Remove useless whitespaces at the beginning and at the end of the line
        line_count = line_count + 1

        # Cut empty lines
        if line == "":
            continue

        # Skip only the first line, where are located the name of the columns
        if firstLine:
            firstLine = False
            continue

        garageCode = line[4]
        producer.send('parking', key="", value={garageCode: line})
        sleep(5)

