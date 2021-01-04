#!/bin/bash

set -x

#Setting variables for Go
#sed -i -e '1iexport PATH=$PATH:$GOROOT/bin:$GOPATH/bin' .bashrc
#sed -i -e '1iexport PATH=/home/ec2-user/bin:/home/ec2-user/.local/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin:/usr/games:/usr/local/games:/snap/bin' .bashrc
#sed -i -e '1iexport GOPATH=/home/ec2-user/epaxos' .bashrc
#sed -i -e '1iexport GOROOT=/home/ec2-user/go' .bashrc

#echo "Downlaoding Go"
#wget -nc --quiet https://storage.googleapis.com/golang/go1.8.3.linux-amd64.tar.gz
#tar -xf go1.8.3.linux-amd64.tar.gz

#echo "Running yum update"
#sudo yum -y update

#sudo yum -y install git

#git clone https://github.com/efficient/epaxos.git

#mkdir -p epaxos/bin
#mkdir -p canopus/experiments

echo "Setting up jdk"
cd "$HOME"
wget -nc --quiet https://cs.uwaterloo.ca/~sm3rizvi/packages/jdk11.tgz
tar -xf jdk11.tgz
jdk="jdk-11.0.3"
sudo update-alternatives --install /usr/bin/java java "$HOME"/${jdk}/bin/java 2000
sudo update-alternatives --install /usr/bin/javac javac "$HOME"/${jdk}/bin/javac 2000
sudo update-alternatives --set java "$HOME"/${jdk}/bin/java
sudo update-alternatives --set javac "$HOME"/${jdk}/bin/javac

echo "Installing Maven"
cd $HOME
wget -nc --quiet https://downloads.apache.org/maven/maven-3/3.6.3/binaries/apache-maven-3.6.3-bin.tar.gz
tar -xf apache-maven-3.6.3-bin.tar.gz
maven="apache-maven-3.6.3"
#sed -i -e '1iexport PATH=$PATH:$MAVEN_HOME/bin' .bashrc
#sed -i -e '1iexport MAVEN_HOME=$HOME/${maven}' .bashrc
#echo "export ANT_HOME=$HOME/ant-1.10.7" >> $HOME/.bashrc
echo "export THRIFT_HOME=\$HOME/thrift-0.13.0" >> $HOME/.bashrc
echo "export JAVA_HOME=\$HOME/jdk-11.0.3" >> $HOME/.bashrc
echo "export MAVEN_HOME=\$HOME/${maven}" >> $HOME/.bashrc
echo "export PATH=\$PATH:\$THRIFT_HOME:\$JAVA_HOME/bin:\$MAVEN_HOME/bin:\$HOME/.local/bin" >> $HOME/.bashrc
echo "export LD_LIBRARY_PATH=\$HOME/.local/lib:\$HOME/lib:\$LD_LIBRARY_PATH" >> $HOME/.bashrc
echo "export LD_LIBRARY_PATH=\$LD_LIBRARY_PATH" >> $HOME/.bashrc


source .bashrc
mvn -v

echo "Installing Thrift"
cd $HOME
wget -nc --quiet https://httpd-mirror.sergal.org/apache/thrift/0.13.0/thrift-0.13.0.tar.gz
tar -xf thrift-0.13.0.tar.gz
cd thrift-0.13.0
sudo apt update
sudo apt install -y ant automake bison flex g++ git libboost-all-dev libevent-dev libssl-dev libtool make pkg-config gradle python dstat htop sysstat
./bootstrap.sh
./configure --with-java
sed -i "s/mvn.repo=http:/mvn.repo=https:/" lib/java/gradle.properties
make -j
sudo make install

echo "Downloading and installing Kawkab code"
cd $HOME
mkdir kawkab
cd kawkab
wget -N --quiet https://cs.uwaterloo.ca/~sm3rizvi/packages/kawkab.tgz
tar -xf kawkab.tgz
mvn compile
cd zookeeper
wget -N --quiet https://cs.uwaterloo.ca/~sm3rizvi/packages/zookeeper-3.5.4-beta.tar.gz
tar -xf zookeeper-3.5.4-beta.tar.gz

echo "Making /tmp/kawkab"
mkdir /tmp/kawkab
#sudo umount /tmp/zoobench
#sudo mount -t tmpfs -o size=2G tmpfs /tmp/zoobench

#mkdir /tmp/zoobench/clients
#mkdir /tmp/zoobench/results
#mkdir -p /tmp/zoobench/servers/out

#echo "Installing iperf"
#Installs iperf3 on RHEL based AMI
#---------------------------------
#sudo yum -y install git gcc make
#git clone https://github.com/esnet/iperf
#cd iperf
#./configure
#make -j
#sudo make install
#sudo ldconfig

#echo "Installing htop dstat sysstat"
#sudo yum -y install htop dstat sysstat

#sudo sh -c 'echo "3" > /sys/class/net/eth0/queues/rx-0/rps_cpus'
#sudo sh -c 'echo "c" > /sys/class/net/eth0/queues/rx-1/rps_cpus'


#sudo apt-get -y install htop dstat

#cd $HOME

#workDir="$HOME/lot/experiments/nsdi17/lot5-180c15m-1crd-1kw-ofh2m-1GAgg-50MBbuf-d8-c180-h2-k3-s3-w20/run_1"

#mkdir -p $workDir

mkdir -p kawkab/experiments

echo "..DONE.."
