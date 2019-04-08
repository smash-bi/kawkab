#!/bin/bash

mvn compile -Dtest=AppendTest -DnodeID=0 -DbufferSize=100 -DnumWriters=1 -DdataSize=2500000000 test
