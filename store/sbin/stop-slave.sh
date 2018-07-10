#!/bin/bash

work_pid=`ps -ef | grep Worker | grep carbondata | awk '{print $2}'`

if [[ -n work_pid ]]; then
  kill -9 work_pid
  echo "Work on $(hostname) was stopped"
else
  echo "Work on $(hostname) can't be found"
fi