#!/bin/sh

PROJ_DIR='/home/fprotect/finprotect'
PROJ_SUBDIR="fprotect"

ZK_DIR="zookeeper-3.4.12"

KAFKA_DIR="kafka_2.11-2.0.0"


#############################################################################

cd $PROJ_DIR


#zookeeper
./$ZK_DIR/bin/zkServer.sh start

sleep 3

#kafka
./$KAFKA_DIR/bin/kafka-server-start.sh $KAFKA_DIR/config/fp-server.properties &
##  $KAFKA_DIR/bin/kafka-topics.sh --create --zookeeper localhost:2181 --replication-factor 1 --partitions 1 --topic fp_trdata
$KAFKA_DIR/bin/connect-standalone.sh $KAFKA_DIR/config/fp-connect-standalone.properties $KAFKA_DIR/config/fp-connect-file-source.properties &
##  $KAFKA_DIR/bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic fp_trdata --from-beginning &

#confluent

