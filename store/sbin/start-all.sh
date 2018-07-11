#!/bin/bash

conf="$(cd "`dirname "$0"`/../conf"; pwd)"
./store-env.sh

JAVA=${JAVA_HOME}/bin/java

cd ${CARBON_HOME}/sbin
./start-horizon.sh

if [[ -f "${CARBON_HOME}/conf/slaves" ]]; then
  HOST_LIST=`cat "${CARBON_HOME}/conf/slaves"`
else
  HOST_LIST=localhost
fi

for slave in `echo "HOST_LIST"|sed "s/#.*$//;/^$/d"`; do
    ssh -o StrictHostKeyChecking=no $slave 'cd '${CARBON_HOME}'/sbin; ./start-slave.sh'
done

wait