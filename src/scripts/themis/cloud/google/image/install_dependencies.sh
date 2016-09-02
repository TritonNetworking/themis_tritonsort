#!/bin/bash

set -e

# Don't use strict host key checking for github
echo "Host github.com" >> ~/.ssh/config
echo "  HostName github.com" >> ~/.ssh/config
echo "  StrictHostKeyChecking no" >> ~/.ssh/config
chmod 600 ~/.ssh/config

# Update aptitude
sudo aptitude -y update

# Install packages from aptitude
sudo apt-get install -y less sysstat cmake gcc fio git g++ libjemalloc-dev libboost-dev libboost-system-dev libboost-filesystem-dev libgsl0-dev zlib1g-dev libaio-dev libcurl4-openssl-dev python-pip python-matplotlib gdb subversion mercurial tmux fping libcppunit-dev libgflags-dev iperf libhiredis-dev vnstat python-openssl python-paramiko screen

# Install packages from source
PACKAGE_DIR=~/.packages
mkdir -p $PACKAGE_DIR

sudo apt-get remove -y cmake
cd $PACKAGE_DIR
wget https://cmake.org/files/v3.5/cmake-3.5.2.tar.gz
tar xvzf ./cmake-3.5.2.tar.gz
cd $PACKAGE_DIR/cmake-3.5.2
./bootstrap
make
sudo make install

# Install python packages from pip
sudo pip install redis plumbum networkx PyYAML Jinja2 bottle ecdsa requests

# Update gcloud utility
sudo gcloud components update -q

cd $PACKAGE_DIR
wget https://github.com/google/re2/archive/2015-05-01.tar.gz
tar xvzf ./2015-05-01.tar.gz
cd $PACKAGE_DIR/re2-2015-05-01
make
sudo make install

cd $PACKAGE_DIR
wget https://github.com/jbeder/yaml-cpp/archive/yaml-cpp-0.5.3.tar.gz
tar xvzf ./yaml-cpp-0.5.3.tar.gz
cd $PACKAGE_DIR/yaml-cpp-yaml-cpp-0.5.3
cmake .
make
sudo make install

cd $PACKAGE_DIR
wget https://github.com/open-source-parsers/jsoncpp/archive/1.6.2.tar.gz
tar xvzf ./1.6.2.tar.gz
cd $PACKAGE_DIR/jsoncpp-1.6.2
cmake .
make
sudo make install

# cd $PACKAGE_DIR
# wget http://sourceforge.net/projects/cppunit/files/cppunit/1.12.1/cppunit-1.12.1.tar.gz
# tar xvzf ./cppunit-1.12.1.tar.gz
# cd $PACKAGE_DIR/cppunit-1.12.1
# ./configure
# make
# sudo make install

# cd $PACKAGE_DIR
# wget https://gflags.googlecode.com/files/gflags-2.0.tar.gz
# tar xzvf gflags-2.0.tar.gz
# cd $PACKAGE_DIR/gflags-2.0
# ./configure
# make
# sudo make install

# cd $PACKAGE_DIR
# git clone https://github.com/esnet/iperf.git
# cd $PACKAGE_DIR/iperf
# ./configure
# make
# sudo make install

cd $PACKAGE_DIR
wget ftp://ftp.netperf.org/netperf/netperf-2.7.0.tar.gz
tar xvzf ./netperf-2.7.0.tar.gz
cd $PACKAGE_DIR/netperf-2.7.0
./configure --enable-demo
make
sudo make install
# Start netserver
if ! pgrep "netserver" > /dev/null; then
  /usr/local/bin/netserver
fi

cd $PACKAGE_DIR
wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
tar xvzf ./libunwind-1.1.tar.gz
cd $PACKAGE_DIR/libunwind-1.1
./configure
make
sudo make install

cd $PACKAGE_DIR
wget https://github.com/gperftools/gperftools/releases/download/gperftools-2.1/gperftools-2.1.tar.gz
tar xvzf ./gperftools-2.1.tar.gz
cd $PACKAGE_DIR/gperftools-2.1
./configure
make
sudo make install

cd $PACKAGE_DIR
wget http://download.redis.io/releases/redis-2.8.9.tar.gz
tar xvzf ./redis-2.8.9.tar.gz
cd $PACKAGE_DIR/redis-2.8.9
make
sudo make install

# cd $PACKAGE_DIR
# git clone https://github.com/redis/hiredis.git
# cd $PACKAGE_DIR/hiredis
# make
# sudo make install

# cd $PACKAGE_DIR
# git clone https://github.com/vergoh/vnstat.git
# cd $PACKAGE_DIR/vnstat
# make
# sudo make install

# Needed for loggrep
sudo ldconfig
