#!/bin/bash

set -e
shopt -s failglob

if [ $# -ne 1 ]
then
    echo "Usage: `basename $0` output_dir"
    exit 1
fi

OUTPUT_DIR=$1

if [ ! -d $OUTPUT_DIR ]
then
    echo "Output directory $OUTPUT_DIR doesn't exist."
    exit 1
fi

# Run all scripts in parallel
for SCRIPT in ${OUTPUT_DIR}/*.sh
do
    # Run the script but squash stdout and stderr
    echo "Running $SCRIPT"
    $SCRIPT >/dev/null 2>&1 &
done

# Wait for all scripts to finish.
wait
