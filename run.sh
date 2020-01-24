#!/bin/bash

function usage {
cat << EOF

    Usage: $0 -[ b ] -c

    -b : Submit Basic Job

    -c : Compile with SBT

EOF
    exit 1
}

function spark_runner {
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/${JOB_NAME} ||: \
    && $SPARK_HOME/bin/spark-submit --master ${MASTER} --deploy-mode cluster \
    --class ${JOB_CLASS} --supervise ${JAR_FILE} ${INPUT} ${OUTPUT}
}

# Compile src 
if [[ $* = *-c* ]]; then
    sbt package
    LINES=`find . -name "*.scala" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
fi

# Configuration Details

CORE_HDFS="hdfs://earth:32351"
CORE_SPARK="spark://earth:32365"

MASTER="yarn" # could also be CORE_SPARK
OUT_DIR="/out"
JAR_FILE="target/scala-2.11/wiki-hits_2.11-1.0.jar"

# Various Jobs to Execute
case "$1" in

-b|--basic)
    JOB_NAME="basic"
    JOB_CLASS="cs535.spark.SimpleApp"
    INPUT="${CORE_HDFS}/data/PA1/titles-sorted.txt"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;

*) usage;
    ;;

esac
