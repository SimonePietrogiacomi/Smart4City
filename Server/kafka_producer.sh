#!/bin/bash

# TODO: update this path
kafka_path="~/big_data/kafka_2.11-2.3.0/bin/kafka-console-producer.sh"

sed 1d $dataset_path/pollution/$1 | while read d
do
   echo $d | $kafka_path --broker-list localhost:9092 --topic new_topic
done
