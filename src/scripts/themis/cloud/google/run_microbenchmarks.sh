#!/bin/bash

set -e

# Run fio benchmarks
FIO_DIR=${log_directory}/fio
CONFIG_DIR=${config_directory}/fio
mkdir -p $FIO_DIR

fio --output $FIO_DIR/write.out $CONFIG_DIR/write.ini
fio --output $FIO_DIR/read.out $CONFIG_DIR/read.ini
fio --output $FIO_DIR/rw.out $CONFIG_DIR/rw.ini

# Wipe fio data
rm -rf $disk_mountpoint/disk_0/*

# Run netperf benchmarks from this node (0) to node 1
NETPERF_DIR=${log_directory}/netperf
mkdir -p $NETPERF_DIR
SERVER=cluster-$id-slave-1

netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/1flow_1_of_1 &
wait

netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/2flow_1_of_2 &
netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/2flow_2_of_2 &
wait

netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/3flow_1_of_3 &
netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/3flow_2_of_3 &
netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/3flow_3_of_3 &
wait

netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/4flow_1_of_4 &
netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/4flow_2_of_4 &
netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/4flow_3_of_4 &
netperf -f g -H $SERVER -l 60 -t TCP_STREAM > $NETPERF_DIR/4flow_4_of_4 &
wait

# Upload log files
$themis_directory/src/scripts/themis/cloud/upload_logs.py
