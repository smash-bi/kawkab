Kawkab Distributed Filesystem
for Fast Data

# Setup

## Prerequisites
* Maven
* ZooKeeper 3.5.x
* minio S3 emulator

## Download ZooKeeper

```
  cd zookeeper
  wget http://apache.forsale.plus/zookeeper/zookeeper-3.5.4-beta/zookeeper-3.5.4-beta.tar.gz
  tar -xf zookeeper-3.5.4-beta.tar.gz
```

## Download minio (minio.io)

See the instructions in the [minio](minio) folder

## Run ZooKeeper and minio

### Run ZooKeeper
You can use [run-bg.sh](zookeeper/run-bg.sh) or [run-fg.sh](zookeeper/run-fg.sh) in the zookeeper folder to start a ZooKeeper node. Use [stop-bg.sh](zookeeper/stop-bg.sh) to stop the ZooKeeper nodes running in the background.

Note: The scripts in the [zookeeper](zookeeper) folder are configured for zookeeper-3.5.4-beta. Customize the *zkDir* variable in the scripts to change the ZooKeeper folder.

### Run minio
You can use [run-server.sh](minio/run-server.sh) in the minio folder to start the minio server.

**Note:** You can optionally use [minio/reset.sh](minio/reset.sh) to start both the ZooKeeper and minio nodes from the same script. Please customize the script to configure the path of ZooKeeper folder before use.

## Customize the Fileysystem Configuration
Please customize the [config.properties](src/main/resources/conf.properties) file to customize the filesystem. Specifically, please customize the following variables:
* basePath
* zkMainServers
* minioServers
* The Kawkab nodes count and list

## Run AppendTest

First make directory "fs" in the root folder. The filesystem files will be created in this folder.

To run the append test:
```
  mvn compile
  mvn -Dtest=AppendTest -DnodeID=0 test
```

## Run CLI
```
  mvn exec:java -D"exec.mainClass"="kawkab.fs.cli.CLI"
```

## Notes
* You may want to store the actual data in the temporary directory or some other drive. So you can create symbolic links for the "fs" folder or minio/data folder.

```
ln -s fs /tmp/kawkab/fs
ln -s minio/data /tmp/kawkab/minio-data
```

# Notes
* For subsequent tests, you may have to restart the minio and zookeeper servers. Moroever, delete the filesystem files from fs/\* folders (rm -r fs/\*) if you get some exceptions when running tests again and again. Be carefull with "*rm -r*"!!!.
