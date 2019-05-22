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

rm -r ${logdir}/${server}/*
rm -r ${datadir}/${server}/*
rm -r ${outdir}/${server}/*

exit 0

