#!/bin/sh

# This script copies zookeeper to n instances, sets up the configurations, and runs zookeeper

ssh red07 "cp ~/zookeeper /var/zookeeper/;"
ssh red08 "cp ~/zookeeper /var/zookeeper/;"
ssh red09 "cp ~/zookeeper /var/zookeeper/;"
