#!/bin/bash

if [[ $# -ne 11 ]] ; then
  echo "usage: $0 <apnd|noop|read> <testID> <testDur> <recsPerBatch> <totalClients> <clientsPerJvm> <clientID> <recSize> <run> <mip> <mid>"
  exit 1
fi


tc=$5
nc=$6
cid=$7
wt=5
mid=${11}
mip=${10}
mport=43567
mgc=true
sip=10.10.0.4
sport=33433
bs=$4
rs=$8
nf=6000
typ=$1
tstid=$2
run=$9
td=$3

d=$(pwd)

fip="${d}/experiments/thesis/kawkab/${typ}-${tstid}-nc${tc}-bs${bs}-rs${rs}-nf${nf}/run-${run}"

if [[ ! -e $fip ]]; then
  mkdir -p $fip
fi

#mvn exec:exec -DmainClass=kawkab.fs.testclient.ClientMain -DcmdArgs="cid=0 sip=10.30.0.4 sport=33433 wt=1 mid=0 mip=10.30.0.8 mport=44444 mgc=true nc=$1 bs=$2 rs=42 nf=1"

ld="/hdd1/sm3rizvi/maven-repos"

java -ea -Xms8g -Xmx12g -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -classpath /home/sm3rizvi/kawkab/src/main/resources:/home/sm3rizvi/kawkab/target/classes:${ld}/org/apache/commons/commons-math3/3.6.1/commons-math3-3.6.1.jar:${ld}/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar:${ld}/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:${ld}/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:${ld}/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar:${ld}/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:${ld}/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:${ld}/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:${ld}/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar kawkab.fs.testclient.ClientMain cid=${cid} sip=${sip} sport=${sport} wt=${wt} mid=${mid} mip=${mip} mport=${mport} mgc=${mgc} nc=${nc} bs=${bs} rs=${rs} nf=${nf} fp=${fip} typ=${typ} tc=${tc} td=${td}
