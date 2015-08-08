#!/bin/bash

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` <all|noinput|input> disk_mountpoint"
    exit $E_BADARGS
fi

MODE=$1
DISK_MOUNTPOINT=$2

if [ $MODE != "all" -a $MODE != "noinput" -a $MODE != "input" ]
then
    echo "First arg must be all, noinput, or input"
    exit $E_BADARGS
fi

USERNAME=`whoami`

for disk in ${DISK_MOUNTPOINT}/*
do
    if [ $MODE == "all" -o $MODE == "input" ]
    then
        rm -rf ${disk}/${USERNAME}/inputs &
    fi
    if [ $MODE == "all" -o $MODE == "noinput" ]
    then
        rm -rf ${disk}/${USERNAME}/intermediates &
        rm -rf ${disk}/${USERNAME}/outputs &
        rm -rf ${disk}/benchmarks* &
    fi
done

# Wait for all rm processes to complete
wait
