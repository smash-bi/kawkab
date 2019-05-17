#!/bin/bash

size=2G
disk=/tmp/tmpDisk
mnt=/tmp/kawkab

#touch ${disk}
#truncate -s ${size} ${disk}
#mke2fs -t ext4 -F ${disk}

mkdir ${mnt}
#sudo mount ${disk} ${mnt}
sudo mount -t tmpfs -o size=${size} tmpfs ${mnt}
sudo chown -R $USER:$USER ${mnt}

mkdir ${mnt}/kawkab
mkdir ${mnt}/minio
mkdir ${mnt}/zk

ln -sfn ${mnt}/kawkab fs
ln -sfn ${mnt}/minio minio/data
ln -sfn ${mnt}/zk /tmp/zookeeper
