#!/bin/bash

# TODO: update this path
dataset_path="~/big_data/dataset"

for file in pollution/*.csv
do
	nohup bash $dataset_path/kafka_producer.sh ../$file &
done
