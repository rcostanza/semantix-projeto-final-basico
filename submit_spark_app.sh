#!/bin/bash

readConf () {
    echo $(cat app/settings.cfg | grep $1 | awk -F"=" '{ print $2 }')
}

mkdir build/ -p

cp app/app.py build/
cp app/settings.cfg build/
zip -rj build/spark_app.zip app/

SPARK_CONTAINER=$(readConf "spark_container")

docker cp build/ "${SPARK_CONTAINER}:/home/spark-app"

docker exec $SPARK_CONTAINER bash -c "cd /home/spark-app/; spark-submit --py-files spark_app.zip --archives settings.cfg app.py"
