#!/bin/bash

base=../..

SRC=$base/src
LIB=$base/lib
BIN=$base/bin

x=`find ${SRC} -name "*.java"`
rm -r ${BIN}/kawkab
rsync ${LIB}/*.so ${BIN}
rsync ${LIB}/*.jar ${BIN}

javac -g -cp .:${BIN}:${LIB}: -d ${BIN} $x
