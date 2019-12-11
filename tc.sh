#!/bin/bash

if [[ $# -ne 8 ]] ; then
  echo "usage: $0 <totalClients> <clientsPerJvm> <clientID> <testID> <recsPerBatch> <recSize> <apnd|noop> <run>"
  exit 1
fi


tc=$1
nc=$2
cid=$3
wt=5
mid=1
mip=10.10.0.3
mport=43567
mgc=true
sip=10.10.0.2
sport=33433
bs=$5
rs=$6
nf=10
typ=$7
tstid=$4
run=$8





d=$(pwd)

fip="${d}/experiments/thesis/${typ}-${tstid}-nc${tc}-bs${bs}-rs${rs}-nf${nf}/run-${run}"

if [[ ! -e $fip ]]; then
  mkdir -p $fip
fi

#mvn exec:exec -DmainClass=kawkab.fs.testclient.ClientMain -DcmdArgs="cid=0 sip=10.30.0.4 sport=33433 wt=1 mid=0 mip=10.30.0.8 mport=44444 mgc=true nc=$1 bs=$2 rs=42 nf=1"

ld="/hdd1/sm3rizvi/maven-repos"

java -ea -Xms8g -Xmx12g -XX:+UnlockExperimentalVMOptions -XX:+UseZGC -classpath /home/sm3rizvi/kawkab/src/main/resources:/home/sm3rizvi/kawkab/target/classes:${ld}/org/apache/thrift/libthrift/0.12.0/libthrift-0.12.0.jar:${ld}/org/apache/httpcomponents/httpclient/4.5.6/httpclient-4.5.6.jar:${ld}/commons-codec/commons-codec/1.10/commons-codec-1.10.jar:${ld}/org/apache/httpcomponents/httpcore/4.4.1/httpcore-4.4.1.jar:${ld}/org/slf4j/slf4j-api/1.7.25/slf4j-api-1.7.25.jar:${ld}/com/google/guava/guava/28.1-jre/guava-28.1-jre.jar:${ld}/com/google/guava/failureaccess/1.0.1/failureaccess-1.0.1.jar:${ld}/com/google/guava/listenablefuture/9999.0-empty-to-avoid-conflict-with-guava/listenablefuture-9999.0-empty-to-avoid-conflict-with-guava.jar kawkab.fs.testclient.ClientMain cid=${cid} sip=${sip} sport=${sport} wt=${wt} mid=${mid} mip=${mip} mport=${mport} mgc=${mgc} nc=${nc} bs=${bs} rs=${rs} nf=${nf} fp=${fip} typ=${typ} tc=${tc}
