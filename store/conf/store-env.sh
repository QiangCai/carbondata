#!/bin/bash

# CARBON_HOME
if [[ -z ${CARBON_HOME} ]]; then
  export CARBON_HOME="$(cd "`dirname "$0"`/.."; pwd)"
fi

# JAVA_HOME
if [[ -z ${JAVA_HOME} ]]; then
  echo "Error: JAVA_HOME is not set." 1>&2
  exit 1
fi