#!/bin/bash

conf="$(cd "`dirname "$0"`/../conf"; pwd)"
./store-env.sh

JAVA=${JAVA_HOME}/bin/java

cd ${CARBON_HOME}/sbin

nohup $JAVA -cp "${CARBON_HOME}/jars/*" -Dcarbonstore.conf.file=${CARBON_HOME}/conf/store.conf org.apache.carbondata.horizon.rest.controller.Horizon > nohup.out 2>&1 &
