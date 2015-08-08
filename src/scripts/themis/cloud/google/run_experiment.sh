#!/bin/bash

set -e

if [ $# -ne 5 ]
then
    echo "Usage: `basename $0` BENCHMARK_CONFIG_FILE THEMIS_CONFIG_FILE THEMIS_JOB_CONFIG FILES_PER_DISK SORT_DATA_SIZE"
    exit $E_BADARGS
fi

BENCHMARK_CONFIG_FILE=$1
THEMIS_CONFIG_FILE=$2
THEMIS_JOB_CONFIG=$3
FILES_PER_DISK=$4
SORT_DATA_SIZE=$5

date

# First run microbenchmarks on slave 0
echo "Running microbenchmarks on cluster-$id-slave-0"
ssh -o PasswordAuthentication=no -o StrictHostKeyChecking=no cluster-$id-slave-0 ${themis_directory}/src/scripts/themis/cloud/google/run_microbenchmarks.sh

# Now run themis benchmarks
SLAVES=`${themis_directory}/src/scripts/themis/cluster/cluster_utils.py live`
echo "Running benchmarks on: $SLAVES"

# First run netbench 3 times
echo "Running NetBench 3 times"
LOG_DIR=${log_directory}/networkbench
mkdir -p $LOG_DIR
${themis_directory}/src/tritonsort/benchmarks/networkbench/run_benchmark.py -i 3 -r -c $BENCHMARK_CONFIG_FILE $SLAVES | tee -a ${LOG_DIR}/netbench.results

# Then run diskbench 3 times
echo "Generating DiskBench data"
LOG_DIR=${log_directory}/storagebench
mkdir -p $LOG_DIR
${themis_directory}/src/tritonsort/benchmarks/storagebench/run_benchmark.py -i 1 -w -c $BENCHMARK_CONFIG_FILE $SLAVES

echo "Running DiskBench 3 times"
${themis_directory}/src/tritonsort/benchmarks/storagebench/run_benchmark.py -i 3 --delete_output -s 30 -c $BENCHMARK_CONFIG_FILE $SLAVES | tee -a ${LOG_DIR}/diskbench.results

echo "Wiping disks"
${themis_directory}/src/scripts/themis/cluster/parallel_ssh.py "${themis_directory}/src/scripts/themis/clear_disks.sh all /mnt/disks"

# Upload log files
echo "Uploading log files for benchmarks"
${themis_directory}/src/scripts/themis/cloud/upload_logs.py

# Generate data set for sort
${themis_directory}/src/scripts/themis/cluster/parallel_ssh.py "${themis_directory}/src/scripts/themis/generate_graysort_inputs.py --no_sudo -g -n${FILES_PER_DISK} ${SORT_DATA_SIZE} gensort_2013"

# Start themis coordinator
tmux new-session -d -s job_runner "${themis_directory}/src/scripts/themis/job_runner/cluster_coordinator.py ${themis_directory}/src/tritonsort/mapreduce/mapreduce $THEMIS_CONFIG_FILE"

echo "Sleeping 30 seconds to wait for cluster coordinator script to come online"
sleep 30

echo "Starting sort #1"
${themis_directory}/src/scripts/themis/job_runner/run_job.py $THEMIS_JOB_CONFIG
echo "Sleeping 20 seconds"
sleep 20
echo "Wiping disks"
${themis_directory}/src/scripts/themis/cluster/parallel_ssh.py "${themis_directory}/src/scripts/themis/clear_disks.sh noinput /mnt/disks"
echo "Sleeping 20 seconds"
sleep 20
echo "Starting sort #2"
${themis_directory}/src/scripts/themis/job_runner/run_job.py $THEMIS_JOB_CONFIG
echo "Sleeping 20 seconds"
sleep 20
echo "Wiping disks"
${themis_directory}/src/scripts/themis/cluster/parallel_ssh.py "${themis_directory}/src/scripts/themis/clear_disks.sh noinput /mnt/disks"
echo "Sleeping 20 seconds"
sleep 20
echo "Starting sort #3"
${themis_directory}/src/scripts/themis/job_runner/run_job.py $THEMIS_JOB_CONFIG
echo "Sleeping 20 seconds"
sleep 20
echo "Wiping disks"
${themis_directory}/src/scripts/themis/cluster/parallel_ssh.py "${themis_directory}/src/scripts/themis/clear_disks.sh noinput /mnt/disks"
echo "Sleeping 20 seconds"
sleep 20

tmux kill-session -t job_runner

# Upload log files
echo "Uploading log files for sort"
${themis_directory}/src/scripts/themis/cloud/upload_logs.py

echo "Downloading log files for sort"
${themis_directory}/src/scripts/themis/cloud/download_logs.py

date
