#!/bin/bash

set -e

# Don't use strict host key checking for github
echo "Host github.com" >> ~/.ssh/config
echo "  HostName github.com" >> ~/.ssh/config
echo "  StrictHostKeyChecking no" >> ~/.ssh/config
chmod 600 ~/.ssh/config

# Update yum
sudo yum -y update

# Install packages from yum
# Use gcc48 and python26 for 2014.09 - use anyway but exclude kernel
sudo yum -y install sysstat.x86_64 xfsprogs.x86_64 gcc48.x86_64 fio.x86_64 git.x86_64 cmake.x86_64 gcc48-c++.x86_64 jemalloc-devel.x86_64 boost-devel.x86_64 gsl-devel.x86_64 zlib-devel.x86_64 libaio-devel.x86_64 libcurl-devel.x86_64 openssl-devel.x86_64 python-pip.noarch python-matplotlib.x86_64 gdb.x86_64 subversion.x86_64 mercurial.x86_64 tmux.x86_64 fping.x86_64

# Install python packages from pip
sudo pip install redis plumbum networkx PyYAML Jinja2 bottle

# Install packages from source
PACKAGE_DIR=~/.packages
mkdir -p $PACKAGE_DIR

cd $PACKAGE_DIR
wget https://re2.googlecode.com/files/re2-20140111.tgz
tar xvzf ./re2-20140111.tgz
cd $PACKAGE_DIR/re2
make
sudo make install

cd $PACKAGE_DIR
wget http://sourceforge.net/projects/cppunit/files/cppunit/1.12.1/cppunit-1.12.1.tar.gz
tar xvzf ./cppunit-1.12.1.tar.gz
cd $PACKAGE_DIR/cppunit-1.12.1
./configure
make
sudo make install

cd $PACKAGE_DIR
wget https://gflags.googlecode.com/files/gflags-2.0.tar.gz
tar xzvf gflags-2.0.tar.gz
cd $PACKAGE_DIR/gflags-2.0
./configure
make
sudo make install

cd $PACKAGE_DIR
git clone https://github.com/esnet/iperf.git
cd $PACKAGE_DIR/iperf
./configure
make
sudo make install

cd $PACKAGE_DIR
wget ftp://ftp.netperf.org/netperf/netperf-2.6.0.tar.gz
tar xvzf ./netperf-2.6.0.tar.gz
cd $PACKAGE_DIR/netperf-2.6.0
./configure --enable-demo
make
sudo make install
# Start netserver
/usr/local/bin/netserver

cd $PACKAGE_DIR
wget http://download.savannah.gnu.org/releases/libunwind/libunwind-1.1.tar.gz
tar xvzf ./libunwind-1.1.tar.gz
cd $PACKAGE_DIR/libunwind-1.1
./configure
make
sudo make install

cd $PACKAGE_DIR
wget https://gperftools.googlecode.com/files/gperftools-2.1.tar.gz
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

cd $PACKAGE_DIR
git clone https://github.com/redis/hiredis.git
cd $PACKAGE_DIR/hiredis
make
sudo make install

cd $PACKAGE_DIR
git clone https://github.com/vergoh/vnstat.git
cd $PACKAGE_DIR/vnstat
make
sudo make install

# Needed for loggrep
sudo bash -c "echo /usr/local/lib > /etc/ld.so.conf.d/locallib.conf"
sudo ldconfig
