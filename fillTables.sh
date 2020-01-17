#!/bin/sh

spark-submit --class Dates --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 dates.jar 2000 2018
echo "zasilono tabele dates"

spark-submit --class VehicleTypes --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 vehicle-types.jar
echo "zasilono tabele vehicle_types"
spark-submit --class Weathers --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 weathers.jar projekt/input/uk-trafic/
echo "zasilono tabele weathers"

spark-submit --class Authorities --master yarn --num-executors 5 --driver-memory 512m \
    --executor-memory 512m --executor-cores 1 authorities.jar projekt/input/uk-trafic/
echo "zasilono tabele authorities"

spark-submit --class Facts --master yarn --num-executors 5 --driver-memory 1024m \
    --executor-memory 1024m --executor-cores 1 facts.jar projekt/input/uk-trafic/
echo "zasilono fakty"
    
