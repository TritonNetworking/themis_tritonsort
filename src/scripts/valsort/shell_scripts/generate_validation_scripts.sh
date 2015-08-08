#!/bin/bash

set -e
shopt -s failglob

if [ $# -lt 4 ]
then
    echo "Usage: `basename $0` output_dir valsort_binary parallelism input_dir_1 [input_dir_2 [ input_dir_3 .... ]]"
    exit 1
fi

OUTPUT_DIR=$1
VALSORT_BINARY=$2
PARALLELISM=$3

# Make the list of input directories appear to be the entire argument list
shift 3

# Create output dir if it doesn't exist
mkdir -p $OUTPUT_DIR

# Quickly check that all input directories exist before looking for files.
for INPUT_DIR in $*
do
    if [ ! -d $INPUT_DIR ]
    then
        echo "Input directory $INPUT_DIR doesn't exist."
        exit 1
    fi
done

INPUT_DIR_ID=1
for INPUT_DIR in $*
do
    # Set up parallel validations scripts for this input directory.
    for PARALLEL_ID in `seq 1 $PARALLELISM`
    do
        PARALLEL_SCRIPT=${OUTPUT_DIR}/${INPUT_DIR_ID}_${PARALLEL_ID}.sh

        # Remove it if it already exists.
        if [ -f $PARALLEL_SCRIPT ]
        then
            rm $PARALLEL_SCRIPT
        fi

        # Set up a new shell script
        touch $PARALLEL_SCRIPT
        chmod u+x $PARALLEL_SCRIPT
        echo -e "#!/bin/bash\n" > $PARALLEL_SCRIPT
    done

    # Generate validation scripts for this input directory up to the levels of
    # parallelism specified
    FILE_COUNT=0
    for i in $INPUT_DIR/*.partition
    do
        # Figure out which parallel script this file belongs to
        # Need to add 1 because expr returns 1 if the answer is 0...
        PARALLEL_ID=`expr $FILE_COUNT % $PARALLELISM + 1`
        PARALLEL_SCRIPT=${OUTPUT_DIR}/${INPUT_DIR_ID}_${PARALLEL_ID}.sh

        echo "Assigning $i to script ${INPUT_DIR_ID}_${PARALLEL_ID}.sh"

        # Compute sumfile name
        BASENAME=`basename $i`
        SUMFILE=${OUTPUT_DIR}/${BASENAME}.sumfile

        # Add a line to the script that actually runs the valsort
        echo "echo $i; if [ ! -f $SUMFILE ]; then echo $VALSORT_BINARY -o $SUMFILE $i; $VALSORT_BINARY -o $SUMFILE $i; fi" >> $PARALLEL_SCRIPT

        # Increment count for next file
        FILE_COUNT=`expr $FILE_COUNT + 1`
    done

    INPUT_DIR_ID=`expr $INPUT_DIR_ID + 1`
done
