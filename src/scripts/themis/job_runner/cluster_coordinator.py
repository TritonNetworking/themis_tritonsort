#!/usr/bin/env python

import sys, argparse, json, logging, time, string, \
    os, subprocess, getpass, yaml, hashlib, signal, redis_utils, random, \
    shutil, select, ConfigParser
from input_file_utils import generate_read_requests, gather_input_file_paths, \
    load_read_requests
from coordinator_utils import create_log_directory, create_batch_directory

from plumbum.cmd import uname

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(name)-15s %(message)s",
    datefmt="%m-%d %H:%M:%S")

log = logging.getLogger("themis_coordinator")
log.setLevel(logging.DEBUG)

CLUSTER_CONF = os.path.expanduser("~/cluster.conf")
if not os.path.exists(CLUSTER_CONF):
    print >>sys.stderr, "Could not find ~/cluster.conf"
    sys.exit(1)

def print_keyboard_commands():
    log.info("Keyboard commands:\n  "
             "barrier/b - Print list of nodes that have yet to reach the "
             "current barrier\n  "
             "help/h - Display this help information\n  "
             "running/r - Print list of running nodes\n  "
             "time/t - Print the running time of the current phase"
             )

def read_request_msg(job_ids, file_url, offset, length):
    return json.dumps({
            "type" : 0,
            "job_ids" : job_ids,
            "path" : file_url,
            "offset" : offset,
            "length" : length
            })

def halt_request_msg():
    return json.dumps({"type" : 1})

class ClusterCoordinator(object):
    def __init__(self, redis_host, redis_port, redis_db, config, themis_binary,
                 log_directory, keepalive_refresh,
                 keepalive_timeout, profiler, profiler_options, ld_preload,
                 interfaces):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db
        self.config_file = config
        with open(config, 'r') as fp:
            self.config = yaml.load(fp)
        self.themis_binary = themis_binary
        self.log_directory = log_directory
        self.keepalive_refresh = keepalive_refresh
        self.keepalive_timeout = keepalive_timeout
        self.profiler = profiler
        self.profiler_options = profiler_options
        self.ld_preload = ld_preload
        self.batch_nonce = random.randint(0, 1000000000)
        self.batch_phase_info = {}
        self.interfaces = interfaces

        self.node_coordinator_log_dir = os.path.join(
            log_directory, "node_coordinators")

        self.coordinator_db = redis_utils.CoordinatorDB(
            redis_host, redis_port, redis_db)

        self.ssh_command = utils.ssh_command()

    def check_node_liveness(self):
        node_failed = False

        for host in self.coordinator_db.known_nodes:
            presumed_alive = host in self.coordinator_db.live_nodes
            keepalive_refreshed = self.coordinator_db.keepalive_refreshed(host)

            if keepalive_refreshed and not presumed_alive:
                log.info("Host '%s', thought dead, is now alive again" % (host))
                self.coordinator_db.declare_host_alive(host)
            elif not keepalive_refreshed and presumed_alive:
                log.error(("Keepalive for \"%s\" keepalive expired; "
                           "presumed dead") % (host))
                self.coordinator_db.declare_host_dead(host)
                node_failed = True

                # Note that the node failed for each incomplete batch
                incomplete_batches = self.coordinator_db.incomplete_batches

                for incomplete_batch in incomplete_batches:
                    self.setup_recovery_from_node_failure(
                        incomplete_batch, host)
                    self.coordinator_db.mark_batch_complete(incomplete_batch)

        if node_failed:
            self.fail_incomplete_batches("Node coordinator failed during job")

    def handle_node_failure_reports(self):
        while True:
            failure_report = self.coordinator_db.next_failure_report()

            if failure_report is None:
                return
            elif len(failure_report) == 0:
                log.error("Failed to decode failure report; skipping")
                continue

            batch_id = failure_report["batch_id"]
            hostname = failure_report["hostname"]
            failed_disk = None
            message = failure_report["message"]

            if "disk" in failure_report:
                failed_disk = failure_report["disk"]

            log.info("Received failure report for batch %d from '%s'" %
                     (batch_id, hostname))

            if failed_disk is not None:
                log.info(("Setting up recovery from failure of disk %s on "
                          "node %s") % (failed_disk, hostname))
                self.setup_recovery_from_disk_failure(
                    batch_id, hostname, failed_disk)
            elif message == "internal_report":
                log.info("Setting up recovery from failure of node %s" % (
                        hostname))
                self.setup_recovery_from_node_failure(batch_id, hostname)

            self.fail_batch(batch_id, "On %s: %s" % (hostname, message))

    def check_phase_completion(self):
        for batch_id in self.coordinator_db.incomplete_batches:
            # Pull information about which phase this batch is currently in.
            if batch_id in self.batch_phase_info:
                (current_phase, num_completed_nodes, start_time) = \
                    self.batch_phase_info[batch_id]
                # Check for nodes that completed this phase.
                completed_node = self.coordinator_db.completed_node_for_phase(
                    batch_id, current_phase)
                while completed_node != None:
                    num_completed_nodes += 1
                    total_nodes = len(self.coordinator_db.known_nodes)
                    log.info("  Node %s completed %s (%d / %d)" % (
                            completed_node, current_phase, num_completed_nodes,
                            total_nodes))

                    if num_completed_nodes == total_nodes:
                        # We completed this phase. Log timing information.
                        stop_time = time.time()
                        log.info("%s completed in %.2f seconds" % (
                                current_phase, stop_time - start_time))
                        # Update redis job info
                        # \TODO (MC): This isn't totally accurate for phase zero
                        # since we run each job serially, but it's good enough
                        # for now.
                        for job_id in self.coordinator_db.batch_jobs(batch_id):
                            self.coordinator_db.update_job_status(
                                job_id, { "%s_start_time" % current_phase :
                                              start_time,
                                          "%s_stop_time" % current_phase :
                                              stop_time,
                                          "%s_elapsed_time" % current_phase :
                                              stop_time - start_time})

                        # Move on to the next phase.
                        next_phase = {"phase_zero" : "phase_one",
                                      "phase_one" : "phase_two",
                                      "phase_two" : "phase_three",
                                      "phase_three" : None}
                        current_phase = next_phase[current_phase]
                        self.coordinator_db.begin_phase(batch_id, current_phase)
                        if current_phase != None:
                            log.info("Running %s..." % current_phase)
                            print_keyboard_commands()
                        num_completed_nodes = 0
                        start_time = time.time()

                    # Save updated phase information
                    if current_phase != None:
                        self.batch_phase_info[batch_id] = (
                            current_phase, num_completed_nodes, start_time)
                    else:
                        # We're done, so delete this information from the dict
                        del self.batch_phase_info[batch_id]

                    # Check for another node completion.
                    completed_node = \
                        self.coordinator_db.completed_node_for_phase(
                        batch_id, current_phase)

    def check_keyboard_input(self):
        if select.select([sys.stdin,],[],[],0.0)[0]:
            stdin = sys.stdin.readline()
            stdin = stdin.strip()

            displayed = False
            if stdin == "running" or stdin == "r":
                # Print a list of nodes that are still running the current
                # phase.
                for batch_id in self.coordinator_db.incomplete_batches:
                    # Pull information about which phase this batch is
                    # currently in.
                    if batch_id in self.batch_phase_info:
                        (current_phase, num_completed_nodes, start_time) = \
                            self.batch_phase_info[batch_id]
                        running_nodes = list(
                            self.coordinator_db.query_running_nodes(
                                batch_id, current_phase))
                        log.info("Batch %d: %d nodes currently running %s: "
                                 "%s" % (batch_id, len(running_nodes),
                                         current_phase, running_nodes))
                        displayed = True

                if not displayed:
                    log.info("No nodes are currently running")

            elif stdin == "barrier" or stdin == "b":
                # Display information about the current barrier for each batch.
                for batch_id in self.coordinator_db.incomplete_batches:
                    # Pull information about which phase this batch is
                    # currently in.
                    if batch_id in self.batch_phase_info:
                        (current_phase, num_completed_nodes, start_time) = \
                            self.batch_phase_info[batch_id]

                        (barrier_name, running_nodes, job_id) = \
                            self.coordinator_db.query_barrier(
                            current_phase, batch_id)

                        if barrier_name != None:
                            log_line = "Barrier %s" % barrier_name
                            if job_id != None:
                                log_line += " job %d" % job_id

                            log.info("%s waiting for nodes: %s" % (
                                    log_line, running_nodes))
                            displayed = True

                if not displayed:
                    log.info("No nodes waiting on barriers")
            elif stdin == "time" or stdin == "t":
                for batch_id in self.coordinator_db.incomplete_batches:
                    # Pull information about which phase this batch is
                    # currently in.
                    if batch_id in self.batch_phase_info:
                        (current_phase, num_completed_nodes, start_time) = \
                            self.batch_phase_info[batch_id]

                        log.info("Batch %d running %s for %.2f seconds" % (
                                batch_id, current_phase,
                                time.time() - start_time))
                        displayed = True

                if not displayed:
                    log.info("No jobs currently running")
            elif stdin == "help" or stdin == "h":
                print_keyboard_commands()
            else:
                log.info("Unknown command '%s'" % stdin)

    def setup_next_batch(self):
        job_ids = []
        job_inputs = []

        job_descriptions = self.coordinator_db.next_job()

        for job_description, job_id in job_descriptions:
            log.info("Received job '%s' (job ID %d)" % (
                    job_description["job_name"], job_id))

            if "recovering" in job_description:
                recovering_job = job_description["recovering"]

                log.info("Job %d is recovering failed job %d" %
                         (job_id, recovering_job))

                self.coordinator_db.setup_recovery_job(job_id, recovering_job)

            worker_inputs = self.setup_new_job(job_description, job_id)

            if worker_inputs is not None:
                job_ids.append(job_id)
                job_inputs.append(worker_inputs)

        return (job_ids, job_inputs)

    def run(self):
        self.start_node_coordinators()

        # Clear the job queue of any jobs left over accidentally
        self.coordinator_db.clear_job_queue()

        # Make sure the entire cluster is ping-able
        self.check_node_liveness()
        hosts = list(self.coordinator_db.live_nodes)
        for host in hosts:
            log.info("Asking %s to ping the cluster" % host)
            self.coordinator_db.send_ping_request(host)

        for host in hosts:
            log.info("Waiting for ping results from %s" % host)
            unreachable_nodes = self.coordinator_db.wait_for_ping_reply(host)
            # Expect a string formatted with newline delimiters
            unreachable_nodes = unreachable_nodes.strip()
            unreachable_nodes = unreachable_nodes.split("\n")
            if unreachable_nodes != [""]:
                log.info("Node %s all-cluster ping failed. Unreachable " \
                             "nodes: %s" % (host, unreachable_nodes))
                return

        log.info("Coordinator's main loop starting")
        try:
            while True:
                self.check_node_liveness()

                if len(self.coordinator_db.live_nodes) == 0:
                    log.error("No live nodes remain. Shutting down.")
                    return

                self.handle_node_failure_reports()

                self.check_phase_completion()

                self.check_keyboard_input()

                self.complete_existing_batches()

                batch_jobs, batch_inputs = self.setup_next_batch()

                if len(batch_jobs) > 0:
                    self.run_batch(batch_jobs, batch_inputs)

                self.handle_failures()

                time.sleep(0.5)
        except KeyboardInterrupt:
            log.info("Caught keyboard interrupt; exiting")
            return

    def run_batch(self, batch_jobs, batch_inputs):
        batch_id = self.coordinator_db.next_batch_id

        log.info("Running batch %d with the following job(s): %s" %
                 (batch_id, ', '.join(map(str, batch_jobs))))


        # Create log directory for the current batch
        batch_logs = create_batch_directory(self.log_directory, batch_id)

        # Copy description files to the log directory
        description_dir = os.path.join(
            os.path.dirname(__file__), os.pardir, os.pardir, os.pardir,
            "tritonsort", "mapreduce", "description")
        shutil.copy(os.path.join(description_dir, "stages.json"), batch_logs)
        shutil.copy(os.path.join(description_dir, "structure.json"), batch_logs)

        # Copy config file to log directory
        shutil.copy(self.config_file, batch_logs)

        self.ready_for_next_batch = False

        # Pull out relevant phase zero parameters
        phase_zero_sample_rate = 1 # Sample 100% by default
        if "SAMPLE_RATE" in self.config:
            phase_zero_sample_rate = float(self.config["SAMPLE_RATE"])
        phase_zero_sample_points_per_file = 1 # Sample prefixes by default
        if "SAMPLES_PER_FILE" in self.config:
            phase_zero_sample_points_per_file = \
                int(self.config["SAMPLES_PER_FILE"])
        fixed_key_length = None
        if "MAP_INPUT_FIXED_KEY_LENGTH" in self.config:
            fixed_key_length = int(self.config["MAP_INPUT_FIXED_KEY_LENGTH"])
        fixed_value_length = None
        if "MAP_INPUT_FIXED_VALUE_LENGTH" in self.config:
            fixed_value_length = \
                int(self.config["MAP_INPUT_FIXED_VALUE_LENGTH"])

        # If the application config file (yaml) or the job spec file (json)
        # skips a phase, we should not load read requests for that phase. The
        # job spec file should override the application config file.
        skip_phase_zero = 0
        skip_phase_one = 0
        skip_phase_two = 0
        skip_phase_three = 0
        if "SKIP_PHASE_ZERO" in self.config and self.config["SKIP_PHASE_ZERO"]:
            skip_phase_zero = 1
        if "SKIP_PHASE_ONE" in self.config and self.config["SKIP_PHASE_ONE"]:
            skip_phase_one = 1
        if "SKIP_PHASE_TWO" in self.config and self.config["SKIP_PHASE_TWO"]:
            skip_phase_two = 1
        if "SKIP_PHASE_THREE" in self.config and \
                self.config["SKIP_PHASE_THREE"]:
            skip_phase_three = 1

        # The run_job.py script verifies that all jobs in the batch have the
        # same value of these skip parameters in the job specs, so we can just
        # check the first one.
        for key, value in (
            self.coordinator_db.job_params(batch_jobs[0]).items()):
            if key == "SKIP_PHASE_ZERO":
                skip_phase_zero = value
            if key == "SKIP_PHASE_ONE":
                skip_phase_one = value
            if key == "SKIP_PHASE_TWO":
                skip_phase_two = value
            if key == "SKIP_PHASE_THREE":
                skip_phase_three = value
            if key == "MAP_INPUT_FIXED_KEY_LENGTH":
                fixed_key_length = int(value)
            if key == "MAP_INPUT_FIXED_VALUE_LENGTH":
                fixed_value_length = int(value)

        fixed_tuple_length = None
        if fixed_key_length != None and fixed_value_length != None:
            fixed_tuple_length = fixed_key_length + fixed_value_length

        use_replication = False
        if "OUTPUT_REPLICATION_LEVEL" in self.config and \
                int(self.config["OUTPUT_REPLICATION_LEVEL"]) > 1:
            use_replication = True

        phases = []
        if not skip_phase_zero:
            phases.append(0)
        if not skip_phase_one:
            phases.append(1)
        if not skip_phase_two and use_replication:
            # If we're using replication, phase two will have network transfer,
            # use barriers to guarantee sockets are connected.
            phases.append(2)
        if not skip_phase_three and use_replication:
            # If we're using replication, phase three will have network
            # transfer, use barriers to guarantee sockets are connected.
            phases.append(3)

        # Setup barriers
        self.coordinator_db.create_barriers(phases, batch_id, batch_jobs)

        # Generate read requests for the jobs in the batch
        read_requests = generate_read_requests(
            job_inputs = batch_inputs,
            phase_zero_sample_rate = phase_zero_sample_rate,
            phase_zero_sample_points_per_file =\
                phase_zero_sample_points_per_file,
            tuple_start_offset = fixed_tuple_length,
            job_ids = batch_jobs, phases=phases)

        # Load read requests into read request queue for each worker
        load_read_requests(self.coordinator_db, read_requests)

        start_time = time.time()
        # Mark phase zero as starting now.
        self.coordinator_db.begin_phase(batch_id, "phase_zero")
        self.batch_phase_info[batch_id] = ("phase_zero", 0, start_time)
        log.info("Running phase_zero...")
        print_keyboard_commands()

        for job_id in batch_jobs:
            self.coordinator_db.update_job_status(
                job_id, { "start_time" : str(start_time),
                          "batch_id" : batch_id,
                          "date" : time.asctime()})

        self.coordinator_db.add_jobs_to_batch(batch_id, batch_jobs)

        self.coordinator_db.mark_batch_incomplete(batch_id)

        # Setting current_batch will cause all node coordinators to start work
        # on that batch
        self.coordinator_db.add_batch_to_node_coordinator_batch_queues(batch_id)

    def fail_incomplete_batches(self, fail_message):
        for batch_id in self.coordinator_db.incomplete_batches:
            self.fail_batch(batch_id, fail_message)

    def fail_batch(self, batch_id, fail_message):
        # If a batch has already failed, don't go through the process of
        # cleaning it up a second time
        if self.coordinator_db.batch_failed(batch_id):
            return

        # Mark each job in the batch as failed
        batch_jobs = self.coordinator_db.batch_jobs(batch_id)

        for job_id in batch_jobs:
            self.fail_job(job_id, fail_message)

        # Mark the batch as failed
        self.coordinator_db.fail_batch(batch_id)

    def fail_job(self, job_id, fail_message):
        log.error("Job %d failed" % (job_id))
        field_changes = {
            "fail_message" : fail_message,
            "stop_time" : str(time.time())
            }

        self.coordinator_db.update_job_status(
            job_id, field_changes,
            pre_status="In Progress", post_status="Failed")

    def complete_existing_batches(self):
        for batch_id in self.coordinator_db.incomplete_batches:
            remaining_nodes = self.coordinator_db.remaining_nodes_running_batch(
                batch_id)

            if remaining_nodes is not None and int(remaining_nodes) == 0:
                self.coordinator_db.mark_batch_complete(batch_id)

                for job_id in self.coordinator_db.batch_jobs(batch_id):
                    # Perform a test-and-set on the job_info hash's status key,
                    # only setting it to complete if it was originally "In
                    # Progress"

                    # Compute the overall data processing rate.
                    stop_time = time.time()
                    job_info = self.coordinator_db.job_info(job_id)
                    start_time = float(job_info["start_time"])

                    runtime = stop_time - start_time
                    total_data_in_MB = \
                        float(job_info["total_input_size_bytes"]) / 1000000.0
                    MBps = total_data_in_MB / runtime

                    TBpm = (MBps * 60) / 1000000

                    num_nodes = len(self.coordinator_db.live_nodes)
                    MBps_per_node = MBps / num_nodes

                    self.coordinator_db.update_job_status(
                        job_id, { "stop_time" : str(stop_time),
                                  "runtime" : str(runtime),
                                  "MBps" : str(MBps),
                                  "num_nodes" : str(num_nodes),
                                  "MBps_per_node" : str(MBps_per_node),
                                  "TBpm" : str(TBpm)},
                        pre_status="In Progress", post_status="Complete")

                    log.info(
                        "Job %d completed in %.2f seconds (%.2f MBps / %.2f "
                        "MBps per node / %.2f TBpm)" % (
                            job_id, runtime, MBps, MBps_per_node, TBpm))

                    job_info = self.coordinator_db.job_info(job_id)
                    phase_zero_time = float(job_info["phase_zero_elapsed_time"])
                    phase_one_time = float(job_info["phase_one_elapsed_time"])
                    phase_two_time = float(job_info["phase_two_elapsed_time"])
                    phase_three_time = float(
                        job_info["phase_three_elapsed_time"])

                    log.info(
                        "Phase zero %.2f seconds. Phase one %.2f seconds. "
                        "Phase two %.2f seconds. Phase three %.2f seconds" % (
                            phase_zero_time, phase_one_time, phase_two_time,
                            phase_three_time))

                    # Dump job info to the batch directory in json format.
                    batch_logs = os.path.join(
                        self.log_directory, "run_logs", "batch_%d" % (batch_id))
                    job_info_fp = open(
                        os.path.join(batch_logs, "job_info_%d.json" % (job_id)),
                        "w")

                    json.dump(job_info, job_info_fp, indent=2)
                    job_info_fp.close()

                    # Additionally dump timing and throughputs pretty-printed to
                    # a file.
                    results_fp = open(
                        os.path.join(batch_logs, "results.job_%d" % (job_id)),
                        "w")
                    results_fp.write(
                        "Total time: %.2f seconds\nPhase zero: %.2f seconds\n"
                        "Phase one:  %.2f seconds\nPhase two:  %.2f seconds\n"
                        "Phase three %.2f seconds\n\n"
                        "Throughput: %.2f MB/s (%.2f TB/min)\n"
                        "Per-Server: %.2f MB/s/node" % (
                            runtime, phase_zero_time, phase_one_time,
                            phase_two_time, phase_three_time, MBps, TBpm,
                            MBps_per_node))
                    results_fp.close()

                    # Dump /proc/cpuinfo
                    with open("/proc/cpuinfo", "r") as in_fp:
                        cpuinfo = in_fp.read()
                        with open(os.path.join(batch_logs, "cpuinfo"), "w") as out_fp:
                            out_fp.write(cpuinfo)

                    # Dump uname -a
                    (uname["-a"] > os.path.join(batch_logs, "uname.out"))()

    def setup_recovery_from_node_failure(self, batch_id, host):
        # We need to recover partition ranges that encompass all the host's
        # local disks

        num_disks = len(self.coordinator_db.local_disks(host))

        if num_disks == 0:
            log.error("Can't find local disk list for '%s'; aborting recovery" %
                      (host))
            return

        partition_ranges = self.get_intermediate_partition_ranges(
            batch_id, host, range(num_disks))

        for job_id, ranges in partition_ranges.items():
            self.add_recovery_partition_ranges_to_job(job_id, ranges)

    def setup_recovery_from_disk_failure(self, batch_id, host, failed_disk):
        try:
            disk_id = self.coordinator_db.local_disks(host).index(failed_disk)
        except ValueError:
            log.error("Can't find failed disk '%s' in list of local disks" %
                      (failed_disk))
            return

        # Make sure the partition ranges for this disk get re-done on a failure
        partition_ranges = self.get_intermediate_partition_ranges(
            batch_id, host, [disk_id])

        for job_id, ranges in partition_ranges.items():
            self.add_recovery_partition_ranges_to_job(job_id, ranges)

        # Add this disk to the set of disks that have failed
        self.coordinator_db.mark_local_disk_failed(host, failed_disk)

    def add_recovery_partition_ranges_to_job(self, job_id, partition_ranges):
        for range_start, range_stop in partition_ranges:
            log.debug("Adding recovery partition range [%s, %s] for job %d" %
                      (range_start, range_stop, job_id))
            self.coordinator_db.add_recovery_partition_range(
                job_id, range_start, range_stop)

    def get_intermediate_partition_ranges(
        self, batch_id, target_host, target_disk_ids):

        target_disk_ids.sort()

        batch_jobs = self.coordinator_db.batch_jobs(batch_id)

        job_partition_ranges = {}

        for batch_job in batch_jobs:
            log.debug("Getting partition information for job %d" % (batch_job))

            # Load logical disk counts from phase zero log directory

            job_logical_disk_counts_file = os.path.join(
                self.log_directory, "run_logs", "batch_%d" % batch_id,
                "phase_zero_job_%d" % (batch_job),
                "logical_disk_counts.%d" % (batch_job))

            if not os.path.exists(job_logical_disk_counts_file):
                log.error(("Can't recover job %d; can't find logical disk "
                           "counts file '%s'") %
                          (batch_job, job_logical_disk_counts_file))
                continue

            with open(job_logical_disk_counts_file, 'r') as fp:
                logical_disk_counts = json.load(fp)

            current_partition = 0

            partition_ranges = []
            start_partition = None
            end_partition = None

            target_ip = self.coordinator_db.ipv4_address(target_host)

            if target_host == None:
                log.error("Can't find IPv4 address for host '%s'" %
                          (target_host))
                continue

            target_host = target_ip

            log.debug("Target host: %s, target disks: %s" %
                      (target_host, target_disk_ids))

            for host in logical_disk_counts["::ordered_node_list::"]:
                for disk_id, partition_count in enumerate(
                    logical_disk_counts[host]):

                    if host == target_host and disk_id in target_disk_ids:
                        if start_partition is None:
                            start_partition = current_partition
                    elif start_partition is not None:
                        end_partition = current_partition - 1

                        partition_ranges.append(
                            (start_partition, end_partition))

                        start_partition = None
                        end_partition = None

                    current_partition += partition_count

            if start_partition is not None:
                end_partition = current_partition - 1
                partition_ranges.append((start_partition, end_partition))

            job_partition_ranges[batch_job] = partition_ranges

        return job_partition_ranges

    def setup_new_job(self, job_description, job_id):
        # Put intermediates and outputs for this job in their own
        # sub-directories.
        job_description["intermediate_directory"] += "/job_%d" % job_id
        job_description["output_directory"] += "/job_%d" % job_id

        self.coordinator_db.new_job_info(job_id, job_description)

        # Extract a list of all input files from the directory
        # corresponding to the input location
        input_dir = job_description["input_directory"]

        max_input_files_per_disk = None
        if "max_input_files_per_disk" in job_description:
            max_input_files_per_disk = \
                job_description["max_input_files_per_disk"]

        input_path_struct = gather_input_file_paths(
            self.coordinator_db, input_dir, max_input_files_per_disk)

        if input_path_struct is not None:
            (worker_inputs, total_input_size) = input_path_struct
        else:
            worker_inputs = None
            total_input_size = 0

        input_files_error = (worker_inputs is None or
                             len(worker_inputs) == 0 or
                             total_input_size == 0)

        if worker_inputs == None:
            error_msg = "Unable to list input directory '%s'" % (
                input_dir)
        elif len(worker_inputs) == 0:
            error_msg = ("Didn't find any input files in directory '%s' "
                         % (input_dir))
        elif total_input_size == 0:
            error_msg = "Total length of all input files is 0B"

        if input_files_error:
            self.fail_job(job_id, error_msg)
            log.error("Job %d failed: %s" % (job_id, error_msg))

            self.coordinator_db.update_job_status(
                job_id, { "fail_message" : error_msg }, post_status="Failed")
            return None

        self.coordinator_db.update_job_status(
            job_id, { "total_input_size_bytes" : total_input_size })

        return worker_inputs

    def handle_failures(self):
        pass

    def start_node_coordinators(self):
        # Conditionally specify the profiler option
        profiler_option = ""
        if self.profiler is not None:
            profiler_option = "--profiler %s" % self.profiler
            if self.profiler_options is not None:
                # We already use single quotes in the ssh command so we need to
                # use this bash voodoo detailed in
                # http://stackoverflow.com/a/1250279/470771
                profiler_option = "%s --profiler_options '\"'\"'%s'\"'\"'" % (
                    profiler_option, self.profiler_options)

        # Conditionally specify LD_PRELOAD library
        ld_preload = ""
        if self.ld_preload is not None:
            ld_preload = "--ld_preload %s "% self.ld_preload

        # Start node coordinators on each node
        ssh_command_template = string.Template(
            ("%s ${host} 'source /etc/profile; source ~/.bash_profile; "
             "mkdir -p %s; nohup "
             "%s --redis_port=%d --redis_db=%d "
             "--redis_host=%s --keepalive_refresh=%d --keepalive_timeout=%d %s "
             "%s --interfaces %s %s %s ${log_dir} %s 1>${stdout_file} "
             "2>${stderr_file} &'") %
            (self.ssh_command, self.node_coordinator_log_dir,
             os.path.join(SCRIPT_DIR, "node_coordinator.py"),
             self.redis_port, self.redis_db, self.redis_host,
             self.keepalive_refresh, self.keepalive_timeout, profiler_option,
             ld_preload, self.interfaces, self.themis_binary, self.config_file,
             self.batch_nonce))

        for host in self.coordinator_db.known_nodes:
            # Create log directory for node coordinator

            node_coordinator_stdout_file = os.path.join(
                self.node_coordinator_log_dir, "stdout-%s.log" % (host))
            node_coordinator_stderr_file = os.path.join(
                self.node_coordinator_log_dir, "stderr-%s.log" % (host))

            for log_filename in [
                node_coordinator_stdout_file, node_coordinator_stderr_file]:

                utils.backup_if_exists(log_filename)

            ssh_cmd = ssh_command_template.substitute(
                host=host,
                stdout_file = node_coordinator_stdout_file,
                stderr_file = node_coordinator_stderr_file,
                log_dir = self.log_directory)

            # Create a keepalive key for this node coordinator
            self.coordinator_db.create_keepalive(host)

            log.info("Starting node coordinator on '%s'" % (host))
            subprocess.check_call(ssh_cmd, shell=True)

    def stop_node_coordinators(self):
        for host in self.coordinator_db.live_nodes:
            if self.coordinator_db.keepalive_refreshed(host):
                pid = self.coordinator_db.node_coordinator_pid(host)

                if pid != -1:
                    log.info("Killing pid %d on host %s" % (pid, host))

                    cmd = subprocess.Popen(
                        "%s %s kill -s SIGUSR1 %d" %
                        (self.ssh_command, host, pid), shell=True)
                    cmd.communicate()

def spawn_gui_and_check_bind(gui_command, port, out_fp, gui_name):
    # Trick with setsid due to http://stackoverflow.com/a/4791612/470771
    gui_cmd_obj = subprocess.Popen(gui_command, shell=True, stdout=out_fp,
        stderr=subprocess.STDOUT, preexec_fn=os.setsid)

    time.sleep(1)

    # If GUI has exited already, it's because it couldn't bind to
    # the appropriate port
    gui_status = gui_cmd_obj.poll()

    if gui_status is not None:
        if gui_status == 42:
            log.error("%s couldn't bind to port %d" % (gui_name, port))
        else:
            log.error("%s exited with error code %d" % (gui_name, gui_status))
        return None
    else:
        print "Serving %s on port %d" % (gui_name, port)

    return gui_cmd_obj

def start_resource_monitor_gui(args, gui_port):
    with open(args.config, 'r') as fp:
        app_config = yaml.load(fp)

    node_resource_monitor_port = app_config["MONITOR_PORT"]

    cmd_path = os.path.abspath(
        os.path.join(os.path.dirname(__file__), os.pardir, os.pardir,
                     "resource_monitor_gui", "resource_monitor_gui.py"))

    cmd = ("%s --redis_port=%d --redis_db=%d --redis_host=%s --port=%d %d") % (
        cmd_path, args.redis_port, args.redis_db, args.redis_host, gui_port,
        node_resource_monitor_port)

    log_file = os.path.join(args.log_directory, "resource_monitor_gui.log")

    utils.backup_if_exists(log_file)

    out_fp = open(log_file, "w")

    cmd_obj = spawn_gui_and_check_bind(
        cmd, gui_port, out_fp, "resource monitor GUI")

    return (cmd_obj, out_fp)


def start_job_status_gui(args, gui_port):
    log_file = os.path.join(args.log_directory, "web_gui.log")

    utils.backup_if_exists(log_file)

    out_fp = open(log_file, "w")

    cmd = ("%s --redis_port=%d --redis_db=%d --redis_host=%s --port=%d %s") % (
        os.path.join(os.path.dirname(__file__), "job_status.py"),
        args.redis_port, args.redis_db, args.redis_host,
        gui_port, args.log_directory)

    cmd_obj = spawn_gui_and_check_bind(cmd, gui_port, out_fp, "job status GUI")

    return (cmd_obj, out_fp)

def main():
    # Load cluster.conf
    parser = ConfigParser.SafeConfigParser()
    parser.read(CLUSTER_CONF)

    # Get default log directory
    log_directory = parser.get("cluster", "log_directory")

    parser = argparse.ArgumentParser(
        description="coordinates the execution of Themis jobs")
    parser.add_argument("themis_binary", help="path to the Themis binary")
    parser.add_argument("config", help="a YAML file giving configuration "
                        "options for Themis")
    parser.add_argument("--log_directory", "-l",
                        help="the directory in which to store coordinator logs "
                        "(default: %(default)s)", default=log_directory)
    parser.add_argument("--keepalive_refresh", help="the length of time node "
                        "coordinators should wait between refreshing keepalive "
                        "information (default: %(default)s seconds)", type=int,
                        default=2)
    parser.add_argument("--keepalive_timeout", help="the amount of time that "
                        "must pass without receiving a keepalive message from "
                        "a node coordinator before the cluster coordinator "
                        "considers that node to be dead (default: %(default)s "
                        "seconds)", type=int, default=10)
    parser.add_argument("--profiler", help="path to the binary of a profiling"
                        "tool to use, for example valgrind or operf")
    parser.add_argument("--profiler_options", help="options surrounded by "
                        "quotes to pass to the profiler")
    parser.add_argument("--ld_preload", help="Path to a library to be "
                        "preloaded using LD_PRELOAD.")

    utils.add_redis_params(parser)
    utils.add_interfaces_params(parser)

    args = parser.parse_args()

    args.config = os.path.abspath(args.config)

    args.log_directory = create_log_directory(args.log_directory)
    log.info("Logging to %s" % (args.log_directory))

    job_status_gui = None
    job_status_gui_out_fp = None

    resource_monitor_gui = None
    resource_monitor_gui_out_fp = None

    coordinator = None

    try:
        # To make the status GUI port distinct for each user but deterministic
        # for a single user, use 2000 + (the md5 hash of the user's username
        # mod 1000) as the web GUI's port number
        username_md5sum = hashlib.md5()
        username_md5sum.update(getpass.getuser())

        job_status_gui_port = (
            (int(username_md5sum.hexdigest(), 16) % 1000 + 2000) / 10) * 10
        resource_monitor_gui_port = (
            (int(username_md5sum.hexdigest(), 16) % 1000 + 3200) / 10) * 10


        print ""

        # Start the resource monitor web GUI
        resource_monitor_gui, resource_monitor_gui_out_fp = \
            start_resource_monitor_gui(args, resource_monitor_gui_port)

        # Start the job status web GUI
        job_status_gui, job_status_gui_out_fp = start_job_status_gui(
            args, job_status_gui_port)

        print ""

        coordinator = ClusterCoordinator(**vars(args))
        coordinator.run()
    finally:
        if job_status_gui is not None:
            log.info("Stopping job status GUI (PID %d)" % (job_status_gui.pid))
            os.killpg(job_status_gui.pid, signal.SIGTERM)
            job_status_gui.wait()


        if job_status_gui_out_fp is not None:
            job_status_gui_out_fp.flush()
            job_status_gui_out_fp.close()

        if resource_monitor_gui is not None:
            log.info("Stopping resource monitor GUI (PID %d)" % (
                    resource_monitor_gui.pid))
            os.killpg(resource_monitor_gui.pid, signal.SIGTERM)
            resource_monitor_gui.wait()

        if resource_monitor_gui_out_fp is not None:
            resource_monitor_gui_out_fp.flush()
            resource_monitor_gui_out_fp.close()

        if coordinator is not None:
            log.info("Stopping node coordinators")
            coordinator.stop_node_coordinators()

if __name__ == "__main__":
    sys.exit(main())
