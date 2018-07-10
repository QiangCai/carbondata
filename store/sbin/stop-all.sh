#!/bin/bash

if [[ -z ${CARBON_HOME} ]]; then
  export CARBON_HOME="$(cd "`dirname "$0"`/.."; pwd)"
fi

if [[ -f "${CARBON_HOME}/conf/slaves" ]]; then
  HOST_LIST=`cat "${CARBON_HOME}/conf/slaves"`
else
  HOST_LIST=localhost
fi

for slave in `echo "HOST_LIST"|sed "s/#.*$//;/^$/d"`; do
    ssh -o StrictHostKeyChecking=no $slave 'cd '${CARBON_HOME}'/sbin; ./stop-slave.sh'
done

wait