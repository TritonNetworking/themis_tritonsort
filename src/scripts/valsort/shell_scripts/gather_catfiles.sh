#!/bin/bash

set -e

if [ $# -lt 2 ]
then
    echo "Usage: `basename $0` output_dir host_1 [ host_2 [ host_3 .... ]]"
    exit 1
fi

SCRIPT_DIR=`dirname $0`
OUTPUT_DIR=$1
shift 1

if [ ! -d $OUTPUT_DIR ]
then
    echo "Output directory $OUTPUT_DIR doesn't exist."
    exit 1
fi

# Get the ssh command used by this Themis configuration
THEMIS_PYTHON_UTILS_DIR=${SCRIPT_DIR}/../../themis
SCP_COMMAND=`python -c "import sys; sys.path.append('$THEMIS_PYTHON_UTILS_DIR'); import utils; print utils.scp_command()"`

for HOST in $*
do
    # Grab all catfiles from this host and put them in our own output directory
    echo "Gathering files from $HOST..."
    $SCP_COMMAND ${HOST}:${OUTPUT_DIR}/*.catfile ${OUTPUT_DIR}/
done
