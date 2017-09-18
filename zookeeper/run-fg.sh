#!/bin/bash

MINARGS=1

if [ $# -lt "$MINARGS" ]
then
  echo "Usage: $0 <server number>"
  exit 1
fi


datadir="/tmp/zookeeper/data"
logdir="/tmp/zookeeper/logs"
outdir="/tmp/zookeeper/out"
server="server_$1"
conf="conf/zoo$1.cfg"

export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:zookeeper-3.4.10/lib;
export ZOO_LOG_DIR=${logdir}/${server};

#export ZOO_LOG4J_PROP="DEBUG,CONSOLE";
#export ZOO_LOG4J_PROP="INFO,CONSOLE";
export ZOO_LOG4J_PROP="WARN,CONSOLE";

rm -r ${logdir}/${server}/*
rm -r ${datadir}/${server}/*
rm -r ${outdir}/${server}/*

mkdir -p ${datadir}/${server}
echo $1 > ${datadir}/${server}/myid

echo "Starting server $1 with config file: ${conf}"

zookeeper-3.4.10/bin/zkServer.sh start-foreground ${conf}

exit 0
