#!/bin/sh

PROJ_DIR='/home/fprotect/finprotect'
PROJ_SUBDIR="fprotect"

SRC_DIR="$PROJ_SUBDIR/src/main/java/dev/finprotect/"
SRC_PACKAGE='dev.finprotect'
#SRC_CLASS='fpMain'
#SRC_CLASS='fpPrimFilter'
SRC_CLASS='fpSecFilter'

LIB_DIR="$PROJ_SUBDIR/lib"

JAR_DIR="$PROJ_SUBDIR/target"
JAR_FILE="$JAR_DIR/fprotect-0.1.jar"


#############################################################################

cd $PROJ_DIR



#spark
echo "Submitting $JAR_FILE to Spark"
##spark-submit --class "$SRC_PACKAGE.$SRC_CLASS" --jars $LIB_DIR/spark-streaming-kafka-0-10_2.11-2.3.2.jar,$LIB_DIR/kafka_2.11-2.0.0.jar,$LIB_DIR/kafka-clients-2.0.0.jar,$LIB_DIR/spark-streaming-kafka_2.11-1.6.3.jar,$LIB_DIR/spark-sql-kafka-0-10_2.11-2.3.1.jar "$JAR_FILE"
#spark-submit --class "$SRC_PACKAGE.$SRC_CLASS" --conf "spark.executor.extraJavaOptions=-XX:-OmitStackTraceInFastThrow" --conf "spark.driver.extraJavaOptions=-XX:-OmitStackTraceInFastThrow" "$JAR_FILE"

spark-submit --class "$SRC_PACKAGE.$SRC_CLASS" "$JAR_FILE"

#java -cp "fprotect/target/fprotect-0.1.jar" fpPreproc