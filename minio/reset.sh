#!/bin/bash

rm -r data/*
cd ..
cd zookeeper
./stop-bg.sh 1
./stop-bg.sh 2
./stop-bg.sh 3
rm -r /tmp/zookeeper/*
./run-bg.sh 1
./run-bg.sh 2
./run-bg.sh 3
cd ..
cd minio
./run-server.sh
