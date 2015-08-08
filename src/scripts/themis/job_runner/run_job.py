#!/usr/bin/env python

import os, sys, argparse, redis, getpass, socket, time, json

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

def wait_for_job(redis_client, job_name):
    def poll_until(poll_function, success_function, poll_interval):
        while True:
            result = poll_function()

            if success_function(result):
                return result
            else:
                time.sleep(poll_interval)

    job_id = poll_until(
        lambda: redis_client.hget("coordinator_job_id", job_name),
        lambda r: r is not None, 1)

    job_info_key = "job_info:%s" % (job_id)

    batch_id = poll_until(
        lambda: redis_client.hget(job_info_key, "batch_id"),
        lambda r: r is not None, 1)

    batch_remaining_key = "batch_remaining:%s" % (batch_id)

    final_status = poll_until(
        lambda: (redis_client.hget(job_info_key, "status"),
                 redis_client.scard(batch_remaining_key)),
        lambda x: x[0] is not None and x[0] != "In Progress" and x[1] == 0,
        1)

    print "Job '%s' completed with status '%s'" % (job_name, final_status[0])

    if final_status[0] == "Complete":
        return True
    else:
        return False

def run_batch(redis_client, job_specs, non_blocking):
    username = getpass.getuser()
    hostname = socket.getfqdn()

    job_names = []

    for i, job_spec in enumerate(job_specs):
        job_name = "%s@%s:%d:%d" % (username, hostname, int(time.time()), i)

        job_spec["job_name"] = job_name

        job_names.append(job_name)

    redis_client.rpush("job_queue", json.dumps(job_specs))

    all_jobs_succeeded = True

    if not non_blocking:
        for job_name in job_names:
            print "Waiting on completion of job '%s'" % (job_name)

            job_succeeded = wait_for_job(redis_client, job_name)

            all_jobs_succeeded = all_jobs_succeeded and job_succeeded

    return all_jobs_succeeded

def run_job(redis_host, redis_port, redis_db, job_specification_file,
            non_blocking):
    redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db)

    with open(job_specification_file, 'r') as fp:
        job_specs = json.load(fp)

    # If the job spec consists of a single job, treat it as a single batch

    if isinstance(job_specs, dict):
        job_specs = [[job_specs]]

    # If the job spec is a list and its individual items are not lists, it's a
    # single batch. Otherwise, it's an ordered sequence of batches
    if isinstance(job_specs, list):
        is_sequence_of_batches = True

        for spec in job_specs:
            if not isinstance(spec, list):
                is_sequence_of_batches = False
                break

        if not is_sequence_of_batches:
            job_specs = [job_specs]

    # Now that the job spec has been appropriately massaged, run each batch in
    # the list of batches
    for batch in job_specs:
        # Check that every job in the batch runs the same phases.
        # Default to running all phases, but allow the first job to
        # override this choice. Then verify that every subsequent job in the
        # batch has consistent values of the skip parameters.
        skip_phase_zero = False
        skip_phase_one = False
        skip_phase_two = False
        first_job = True
        for job_spec in batch:
            if "params" in job_spec:
                for key, value in job_spec["params"].iteritems():
                    if key == "SKIP_PHASE_ZERO":
                        if first_job:
                            skip_phase_zero = value
                        if value != skip_phase_zero:
                            print "Not all jobs in batch have the same value " \
                                "of SKIP_PHASE_ZERO."
                            return 1
                    if key == "SKIP_PHASE_ONE":
                        if first_job:
                            skip_phase_one = value
                        if value != skip_phase_one:
                            print "Not all jobs in batch have the same value " \
                                "of SKIP_PHASE_ONE."
                            return 1
                    if key == "SKIP_PHASE_TWO":
                        if first_job:
                            skip_phase_two = value
                        if value != skip_phase_two:
                            print "Not all jobs in batch have the same value " \
                                "of SKIP_PHASE_TWO."
                            return 1
            first_job = False

        if not run_batch(redis_client, batch, non_blocking):
            # If one or more jobs in the batch failed, return 1 indicating an
            # error
            return 1

    # All jobs in all batches have succeeded; return 0 indicating success
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="submits a job to the Themis coordinator")
    utils.add_redis_params(parser)
    parser.add_argument("job_specification_file",
                        help="a JSON file giving enough information about the "
                        "job for Themis to run it")
    parser.add_argument("--non_blocking", default=False, action="store_true",
                        help="don't wait for jobs to complete before returning")
    args = parser.parse_args()
    return run_job(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
