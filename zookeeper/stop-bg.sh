#!/bin/bash

MINARGS=1

if [ $# -lt "$MINARGS" ]
then
  echo "Usage: $0 <server number>"
  exit 1
fi

#zkDir=zookeeper-3.4.14
zkDir=zookeeper-3.5.4-beta
BIN=${zkDir}/bin
conf="./conf/zoo$1.cfg"

#export ZOO_LOG4J_PROP="DEBUG,CONSOLE";
#export ZOO_LOG4J_PROP="INFO,CONSOLE";
#export ZOO_LOG4J_PROP="WARN,CONSOLE";

echo "Stopping server $1 with config file: ${conf}"

${BIN}/zkServer.sh stop ${conf}
