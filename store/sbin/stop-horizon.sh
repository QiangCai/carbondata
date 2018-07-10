#!/bin/bash

horizon_pid=`ps -ef | grep Horizon | grep carbondata | awk '{print $2}'`

if [[ -n "horizon_pid" ]]; then
  kill -9 "horizon_pid"
  echo "Horizon on $(hostname) was stopped"
else
  echo "Horizon on $(hostname) can't be found"
fi