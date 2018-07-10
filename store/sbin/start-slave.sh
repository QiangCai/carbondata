#!/bin/bash

conf="$(cd "`dirname "$0"`/../conf"; pwd)"
./store-env.sh

JAVA=${JAVA_HOME}/bin/java

cd ${CARBON_HOME}/sbin

nohup JAVA -cp "${CARBON_HOME}/jars/*" org.apache.carbondata.store.impl.distributed.Worker ${CARBON_HOME}/conf/log4j.properties ${CARBON_HOME}/conf/store.conf > nohup.out 2>&1 &

echo $(hostname) worker started