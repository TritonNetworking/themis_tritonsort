#!/usr/bin/env python

import os, sys, argparse, input_file_utils, redis_utils, json

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

import utils as themis_utils

def reload_read_request_queues(
    job_description_file, job_ids, redis_host, redis_port, redis_db,
    skip_phase_zero, skip_phase_one, phase_zero_sample_size):

    with open(job_description_file, 'r') as fp:
        job_description = json.load(fp)

    coordinator_db = redis_utils.CoordinatorDB(redis_host, redis_port, redis_db)

    input_files = input_file_utils.gather_input_file_paths(
        coordinator_db, job_description["input_directory"])

    phases = []

    if not skip_phase_zero:
        phases.append(0)

    if not skip_phase_one:
        phases.append(1)

    read_requests = input_file_utils.generate_read_requests(
        input_files, phase_zero_sample_size, job_ids, phases)

    input_file_utils.load_read_requests(coordinator_db, read_requests)

def main():
    parser = argparse.ArgumentParser(
        description="set up read request queues to replay a given job's inputs")
    parser.add_argument("job_description_file", help="file describing the job "
                        "whose inputs are to be replayed")
    parser.add_argument(
        "job_ids", nargs="+", help="the job IDs of the jobs being replayed",
        type=int)
    parser.add_argument("--skip_phase_zero", default=False, action="store_true",
                        help="don't generate read requests for phase zero")
    parser.add_argument("--skip_phase_one", default=False, action="store_true",
                        help="don't generate read requests for phase one")
    parser.add_argument("--phase_zero_sample_size", default=125000000,
                        help="how much data to sample from each file in phase "
                        "zero (default: %(default)s)", type=int)

    themis_utils.add_redis_params(parser)

    args = parser.parse_args()
    return reload_read_request_queues(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
