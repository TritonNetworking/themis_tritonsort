#!/bin/bash

set -e

if [ $# -ne 4 ]
then
    echo "Usage: `basename $0` branch recompile(Y/N) debug(Y/N) asserts(Y/N)"
    exit $E_BADARGS
fi

BRANCH=$1
RECOMPILE=$2
DEBUG=$3
ASSERTS=$4

SRC_DIRECTORY=`dirname $0`/../../../
cd $SRC_DIRECTORY

git pull
git checkout $BRANCH

if [[ "$RECOMPILE" == "Y" ]]
then
    if [[ "$DEBUG" == "Y" ]]
    then
        DEBUG="Debug"
    else
        DEBUG="Release"
    fi

    if [[ "$ASSERTS" == "Y" ]]
    then
        ASSERTS="ON"
    else
        ASSERTS="OFF"
    fi

    cmake -D CMAKE_BUILD_TYPE:str=$DEBUG -D ENABLE_ASSERTS:bool=$ASSERTS .
    make clean
    make -j`nproc`
fi
