#!/bin/bash

if [[ $# -ne 1 ]] ; then 
	echo "usage: $0 <nodeID>"
	exit 1
fi

#mvn exec:java -D"exec.mainClass"="kawkab.fs.cli.CLI"
#mvn exec:exec -D"exec.executable"="java -Xmn40g -Xmx40g -DnodeID=1 kawkab.fs.cli.CLI"
mvn exec:exec -DnodeID=$1 -DmainClass=kawkab.fs.cli.CLI
