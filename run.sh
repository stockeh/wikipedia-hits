#!/bin/bash

function usage {
cat << EOF

    Usage: $0 -[ k ] -c

    -k : Key work for "root set"

    -c : Compile with SBT

EOF
    exit 1
}

function remove_hdfs_out {
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/"${JOB_NAME}-base"
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/"${JOB_NAME}-root"
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/"${JOB_NAME}-auth"
    $HADOOP_HOME/bin/hadoop fs -rm -R ${OUT_DIR}/"${JOB_NAME}-hubs"
}

function spark_runner {
    
    remove_hdfs_out
    # --deploy-mode cluster 
    $SPARK_HOME/bin/spark-submit --master ${MASTER} --deploy-mode cluster  \
    --class ${JOB_CLASS} ${JAR_FILE} ${KEY_TOPIC} ${EPSILON} ${INPUT} ${OUTPUT}
}

# Compile src 
if [[ $* = *-c* ]]; then
    sbt package
    LINES=`find . -name "*.scala" -print | xargs wc -l | grep "total" | awk '{$1=$1};1'`
    echo Project has "$LINES" lines
fi

# Configuration Details

CORE_HDFS="hdfs://earth:32351"

# MASTER="yarn"
# MASTER="local"
MASTER="spark://earth.cs.colostate.edu:32365"
OUT_DIR="/out"
JAR_FILE="target/scala-2.11/wiki-hits_2.11-1.0.jar"

# Various Jobs to Execute
case "$1" in

-k|--key)
    KEY_TOPIC="$2"
    EPSILON="0.001"
    JOB_NAME="hits"
    JOB_CLASS="cs535.spark.SimpleApp"
    INPUT="${CORE_HDFS}/data/PA1"
    OUTPUT="${CORE_HDFS}${OUT_DIR}/${JOB_NAME}"
    spark_runner
    ;;

*) usage;
    ;;

esac
