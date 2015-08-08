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

OUTPUT_FILE=${OUTPUT_DIR}/ALLCATFILES
echo "Writing to $OUTPUT_FILE"

if [ -f $OUTPUT_FILE ]
then
    rm $OUTPUT_FILE
fi

# Cat all catfiles
for CATFILE in ${OUTPUT_DIR}/*.catfile
do
    echo $CATFILE
    cat $CATFILE >> $OUTPUT_FILE
done
