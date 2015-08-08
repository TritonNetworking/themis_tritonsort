#!/bin/bash

set -e

if [ $# -ne 2 ]
then
    echo "Usage: `basename $0` output_dir valsort_binary"
    exit 1
fi

OUTPUT_DIR=$1
VALSORT_BINARY=$2

if [ ! -d $OUTPUT_DIR ]
then
    echo "Output directory $OUTPUT_DIR doesn't exist."
    exit 1
fi

ALLCATFILES=${OUTPUT_DIR}/ALLCATFILES

if [ ! -f $ALLCATFILES ]
then
    echo "Final catfile $ALLCATFILES doesn't exist."
    exit 1
fi

$VALSORT_BINARY -s $ALLCATFILES
