import sys
from pyspark import SparkContext
from pyspark.streaming import StreamingContext
from pyspark.streaming.kafka import KafkaUtils
from pyspark.sql import SparkSession
import pymongo


time_interval = 20

longitude_important_square = "10.104986076057457"
latitude_important_square = "56.23172069428216"

if __name__ == "__main__":
    sc = SparkContext(appName="NasuPollutionMeasureAarhus")
    ssc = StreamingContext(sc, time_interval)

    # Mongo
    client = pymongo.MongoClient("localhost", 27017)
    db = client.nasu
    collection = db.aarhus

    brokers, topic = sys.argv[1:]
    kvs = KafkaUtils.createDirectStream(ssc, [topic], {"metadata.broker.list": brokers})
    lines = kvs.map(lambda x: x[1])
    input_pollution = lines.flatMap(lambda line: line.split("\n")) \
        .map(lambda x: x.split(",")) \
        .map(lambda x: (x[0], x[1], x[2], x[3], x[4], x[5], x[6], x[7]))

    def important_square_to_mongo(important_pollution):
        if important_pollution:
            to_mongo = important_pollution.first()
            to_mongo['_id'] = 'important_pollution'
            collection.replace_one({'_id': to_mongo['_id']}, to_mongo, True)

    important_square = input_pollution.filter(lambda x: str(x[5]) == longitude_important_square)\
        .filter(lambda x: str(x[6]) == latitude_important_square)

    important_square.map(lambda x: {'timestamp': x[7], 'ozone': x[0], 'particulate matter': x[1],
                                    'carbon monoxide': x[2], 'sulfur dioxide': x[3], 'nitrogen dioxide': x[4]})\
        .foreachRDD(important_square_to_mongo)

    ozone = input_pollution.map(lambda x: ("ozone", x[0]))
    particulate_matter = input_pollution.map(lambda x: ("particulate matter", x[1]))
    carbon_monoxide = input_pollution.map(lambda x: ("carbon monoxide", x[2]))
    sulfur_dioxide = input_pollution.map(lambda x: ("sulfur dioxide", x[3]))
    nitrogen_dioxide = input_pollution.map(lambda x: ("nitrogen dioxide", x[4]))

    timestamp = input_pollution.map(lambda x: (x[7], "timestamp"))
    # ('2014-08-01 06:05:00', 'timestamp')

    def mean_calculator(type_variable):
        sum_count = type_variable.combineByKey(lambda value: (value, 1),
                                               lambda x, value: (int(x[0]) + int(value), int(x[1]) + 1),
                                               lambda x, y: (x[0] + y[0], x[1] + y[1]))
        # ('ozone', (278, 3))

        average = sum_count.map(lambda x: (x[0], int(x[1][0]) / int(x[1][1])))
        # ('carbon monoxide', 76.66666666666667)

        return average

    ozone_average = mean_calculator(ozone)
    particulate_matter_average = mean_calculator(particulate_matter)
    carbon_monoxide_average = mean_calculator(carbon_monoxide)
    sulfur_dioxide_average = mean_calculator(sulfur_dioxide)
    nitrogen_dioxide_average = mean_calculator(nitrogen_dioxide)
    # ('carbon monoxide', 76.66666666666667)

    ozone_timestamp = timestamp.map(lambda x: ("ozone", x[0]))
    particulate_matter_timestamp = timestamp.map(lambda x: ("particulate matter", x[0]))
    carbon_monoxide_timestamp = timestamp.map(lambda x: ("carbon monoxide", x[0]))
    sulfur_dioxide_timestamp = timestamp.map(lambda x: ("sulfur dioxide", x[0]))
    nitrogen_dioxide_timestamp = timestamp.map(lambda x: ("nitrogen dioxide", x[0]))
    # ('ozone', '2014-08-01 08:15:00')

    ozone_average_timestamp = ozone_average.join(ozone_timestamp) \
        .map(lambda x: (x[1][1], (x[0], x[1][0])))
    particulate_matter_average_timestamp = particulate_matter_average.join(particulate_matter_timestamp) \
        .map(lambda x: (x[1][1], (x[0], x[1][0])))
    carbon_monoxide_average_timestamp = carbon_monoxide_average.join(carbon_monoxide_timestamp) \
        .map(lambda x: (x[1][1], (x[0], x[1][0])))
    sulfur_dioxide_average_timestamp = sulfur_dioxide_average.join(sulfur_dioxide_timestamp) \
        .map(lambda x: (x[1][1], (x[0], x[1][0])))
    nitrogen_dioxide_average_timestamp = nitrogen_dioxide_average.join(nitrogen_dioxide_timestamp) \
        .map(lambda x: (x[1][1], (x[0], x[1][0])))
    # ('2014-08-01 09:45:00', ('ozone', 82.33333333333333))

    current_pollution = ozone_average_timestamp.join(particulate_matter_average_timestamp) \
        .join(carbon_monoxide_average_timestamp) \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][1]))) \
        .join(sulfur_dioxide_average_timestamp) \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][1]))) \
        .join(nitrogen_dioxide_average_timestamp) \
        .map(lambda x: (x[0], (x[1][0][0], x[1][0][1], x[1][0][2], x[1][0][3], x[1][1]))) \
        .map(lambda x: {'timestamp': x[0],
                        'ozone': x[1][0][1], 'ozone variation': '',
                        'particulate matter': x[1][1][1], 'particulate matter variation': '',
                        'carbon monoxide': x[1][2][1], 'carbon monoxide variation': '',
                        'sulfur dioxide': x[1][3][1], 'sulfur dioxide variation': '',
                        'nitrogen dioxide': x[1][4][1], 'nitrogen dioxide variation': ''})

    def to_mongo(pollution):
        last_pollution = collection.find_one()
        to_mongo = pollution.first()
        to_mongo['_id'] = 'average_pollution'
        variation_up = "up"
        variation_down = "down"
        variation_same = "same"
        if last_pollution:

            # ozone
            if float(last_pollution['ozone']) > float(to_mongo['ozone']):
                to_mongo['ozone variation'] = variation_down
            elif float(last_pollution['ozone']) < float(to_mongo['ozone']):
                to_mongo['ozone variation'] = variation_up
            else:
                to_mongo['ozone variation'] = variation_same

            # particulate matter
            if float(last_pollution['particulate matter']) > float(to_mongo['particulate matter']):
                to_mongo['particulate matter variation'] = variation_down
            elif float(last_pollution['particulate matter']) < float(to_mongo['particulate matter']):
                to_mongo['particulate matter variation'] = variation_up
            else:
                to_mongo['particulate matter variation'] = variation_same

            # carbon monoxide
            if float(last_pollution['carbon monoxide']) > float(to_mongo['carbon monoxide']):
                to_mongo['carbon monoxide variation'] = variation_down
            elif float(last_pollution['carbon monoxide']) < float(to_mongo['carbon monoxide']):
                to_mongo['carbon monoxide variation'] = variation_up
            else:
                to_mongo['carbon monoxide variation'] = variation_same

            # sulfur dioxide
            if float(last_pollution['sulfur dioxide']) > float(to_mongo['sulfur dioxide']):
                to_mongo['sulfur dioxide variation'] = variation_down
            elif float(last_pollution['sulfur dioxide']) < float(to_mongo['sulfur dioxide']):
                to_mongo['sulfur dioxide variation'] = variation_up
            else:
                to_mongo['sulfur dioxide variation'] = variation_same

            # nitrogen dioxide
            if float(last_pollution['nitrogen dioxide']) > float(to_mongo['nitrogen dioxide']):
                to_mongo['nitrogen dioxide variation'] = variation_down
            elif float(last_pollution['nitrogen dioxide']) < float(to_mongo['nitrogen dioxide']):
                to_mongo['nitrogen dioxide variation'] = variation_up
            else:
                to_mongo['nitrogen dioxide variation'] = variation_same
        else:
            to_mongo['ozone variation'] = variation_same
            to_mongo['particulate matter variation'] = variation_same
            to_mongo['carbon monoxide variation'] = variation_same
            to_mongo['sulfur dioxide variation'] = variation_same
            to_mongo['nitrogen dioxide variation'] = variation_same
        collection.replace_one({'_id': to_mongo['_id']}, to_mongo, True)

    current_pollution.foreachRDD(to_mongo)

    current_pollution.pprint()
    ssc.start()
    ssc.awaitTermination()
