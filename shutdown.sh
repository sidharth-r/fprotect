#!/bin/sh

PROJ_DIR='/home/fprotect/finprotect'
PROJ_SUBDIR="fprotect"

SPARK_DIR="spark-2.3.1-bin-hadoop2.7"
SPARK_CLASS_PATH="$SPARK_DIR/jars/*"

ZK_DIR="zookeeper-3.4.12"

KAFKA_DIR="kafka_2.11-2.0.0"

#############################################################################

cd $PROJ_DIR

#kafka
./$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic fp_trdata_raw
./$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic fp_trdata
./$KAFKA_DIR/bin/kafka-topics.sh --zookeeper localhost:2181 --delete --topic fp_results

./$KAFKA_DIR/bin/kafka-server-stop.sh

sleep 3

#zookeeper
./$ZK_DIR/bin/zkServer.sh stop


