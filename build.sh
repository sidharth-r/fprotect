#!/bin/sh

PROJ_DIR='/home/fprotect/finprotect'
PROJ_SUBDIR="$PROJ_DIR/fprotect"

SPARK_DIR="$PROJ_DIR/spark-2.3.1-bin-hadoop2.7"
SPARK_LIB_PATH="$SPARK_DIR/jars/*"

KAFKA_DIR="$PROJ_DIR/kafka_2.11-2.0.0"
KAFKA_LIB_PATH="$KAFKA_DIR/libs/*"

SRC_DIR="$PROJ_SUBDIR/src"
SRC_PACKAGE='fprotect'
SRC_CLASS='fpMain'
SRC_FILE="$SRC_DIR/$SRC_PACKAGE/$SRC_CLASS.java"
CLASS_FILE="$SRC_DIR/$SRC_CLASS.class"

LIB_PATH="$PROJ_SUBDIR/lib/*"

BUILD_DIR="$PROJ_SUBDIR/build"
JAR_FILE="$BUILD_DIR/$SRC_PACKAGE.jar"

#############################################################################

echo "Compiling $SRC_FILE..."
javac -cp "$SPARK_LIB_PATH":"$KAFKA_LIB_PATH":"$LIB_PATH" "$SRC_FILE"

cd $SRC_DIR
echo "Creating JAR $JAR_FILE..."
jar cvf "$JAR_FILE" "$SRC_PACKAGE/$SRC_CLASS.class"
