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

# Cat all sumfiles
for SUMFILE in ${OUTPUT_DIR}/*.sumfile
do
    if [ -z $CATFILE ]
    then
        # Extract the partition from the first sumfile, which is named
        # PARTITION_ID.partition.sumfile
        PARTITION_ID=`basename $SUMFILE | awk -F. '{print $1}'`

        # Use this partition ID to name the catfile
        CATFILE=${OUTPUT_DIR}/${PARTITION_ID}.catfile

        if [ -f $CATFILE ]
        then
            rm $CATFILE
        fi

        echo "Writing to $CATFILE"
    fi

    echo $SUMFILE
    cat $SUMFILE >> $CATFILE
done
