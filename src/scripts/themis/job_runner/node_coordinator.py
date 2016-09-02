#!/usr/bin/env python

import os, sys, argparse, logging, socket, subprocess, signal, \
    getpass, shutil, traceback, time, redis_utils, yaml
from plumbum.cmd import fping

from coordinator_utils import TimedIterateThread, create_log_directory,\
    create_batch_directory

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils, monitor_utils

log = logging.getLogger("node_coordinator")
log.setLevel(logging.DEBUG)

class KeepaliveThread(TimedIterateThread):
    def __init__(
        self, hostname, keepalive_refresh, keepalive_timeout, redis_host,
        redis_port, redis_db, pid):

        super(KeepaliveThread, self).__init__(iter_sleep = keepalive_refresh)

        self.coordinator_db = redis_utils.CoordinatorDB(
            redis_host, redis_port, redis_db)

        self.timeout = keepalive_timeout
        self.hostname = hostname

        self.coordinator_db.update_pid(self.hostname, pid)

    def iterate(self):
        self.coordinator_db.refresh_keepalive(self.hostname, self.timeout)

    def cleanup(self):
        log.info("Keepalive thread is down")

class NodeCoordinator(object):
    def __init__(
        self, redis_host, redis_port, redis_db, config, themis_binary,
        log_directory, batch_nonce, keepalive_refresh, keepalive_timeout,
        profiler, profiler_options, ld_preload, interfaces):

        self.redis_host = redis_host
        self.redis_port = redis_port
        self.redis_db = redis_db

        self.coordinator_db = redis_utils.CoordinatorDB(
            redis_host, redis_port, redis_db)

        self.config_file = config
        with open(config, 'r') as fp:
            self.config = yaml.load(fp)
        # Use a default config called defaults.yaml that is stored in the same
        # directory as the binary
        self.default_config = os.path.join(
            os.path.dirname(themis_binary), "defaults.yaml")
        self.themis_binary = themis_binary
        self.log_directory = log_directory
        self.batch_nonce = batch_nonce
        self.profiler = profiler
        self.profiler_options = profiler_options
        self.ld_preload = ld_preload

        self.current_batch = None
        self.ip_address = None
        self.num_interfaces = self.coordinator_db.num_interfaces
        self.interfaces = interfaces

        self.hostname = socket.getfqdn()
        self.username = getpass.getuser()

        self.keepalive_process = KeepaliveThread(
            self.hostname, keepalive_refresh, keepalive_timeout, redis_host,
            redis_port, redis_db, os.getpid())

        self.keepalive_process.start()

    def stop_keepalive(self):
        self.keepalive_process.shutdown()

    def lookup_global_boundary_list(self, job_id, parent_dir):
        recovery_info = self.coordinator_db.recovery_info(job_id)

        boundary_files = []

        if recovery_info is not None:
            # Need to make sure we can recover this job using the
            # boundary list from its previous execution

            recovering_job = recovery_info["recovering_job"]

            boundary_list_file = (
                self.coordinator_db.global_boundary_list_file(
                    recovering_job))

            if boundary_list_file is None:
                log.error("No known disk-backed boundary list file "
                          "for job %d" % (recovering_job))
                return recovering_job

            if not os.path.exists(boundary_list_file):
                log.error("Can't find disk-backed boundary list file "
                          "for job %d '%s'" %
                          (recovering_job, boundary_list_file))
                return recovering_job

            boundary_files.append((recovering_job, boundary_list_file))

        # Need to construct a boundary list file path and make it
        # known to the coordinator DB, then actually construct it
        # in phase zero

        boundary_list_file = os.path.join(
            parent_dir, "global_boundary_list.%d" % (job_id))

        self.coordinator_db.set_global_boundary_list_file(
            job_id, boundary_list_file)

        boundary_files.append((job_id, boundary_list_file))

        return boundary_files

    def lookup_global_boundary_lists(self, job_list, parent_dir):
        global_boundary_list_files = {}

        for job_id in job_list:
            boundary_files = self.lookup_global_boundary_list(
                job_id, parent_dir)

            if type(boundary_files) == int:
                return boundary_files

            for job_id, boundary_file in boundary_files:
                global_boundary_list_files[job_id] = boundary_file

        return global_boundary_list_files


    def run(self):
        # Any pending batches won't be processed by this client
        self.coordinator_db.clear_batch_queue(self.hostname)

        remaining_live_retries = 10

        # Make sure the entire cluster is ping-able
        nodes = list(self.coordinator_db.live_nodes)
        self.coordinator_db.wait_for_ping_request(self.hostname)
        # Issue fping command to the entire cluster
        log.info("Pinging %s" % nodes)
        command = fping["-u"]
        for node in nodes:
            command = command[node]
        unreachable_nodes = command()
        unreachable_nodes = unreachable_nodes.encode("ascii")
        log.info("Unreachable nodes: %s" % unreachable_nodes)
        # Report results to the cluster coordinator
        self.coordinator_db.send_ping_reply(self.hostname, unreachable_nodes)

        while True:
            # Re-grab my node ID, the list of nodes, and the number of
            # intermediate disks on each node
            nodes = list(self.coordinator_db.live_nodes)
            nodes.sort()

            try:
                node_id = nodes.index(self.hostname)
                remaining_live_retries = 10

            except ValueError:
                error_message = (
                    ("Can't find my hostname (%s) in the list of valid "
                     "nodes") % (self.hostname))
                log.error(error_message)

                # Sleep for a little while and try again
                remaining_live_retries -= 1

                if remaining_live_retries == 0:
                    raise RuntimeError(error_message)
                else:
                    time.sleep(1)

                continue

            intermediate_disk_counts = []

            for node in nodes:
                intermediate_disk_counts.append(
                    len(self.coordinator_db.local_disks(node)))

            # Make sure we have the same number of intermediate disks on each
            # node.
            if len(set(intermediate_disk_counts)) != 1:
                error_message = (
                    ("All nodes should have the same number of intermediate "
                     "disks, but counts are %s") % (intermediate_disk_counts))
                log.error(error_message)
                raise RuntimeError(error_message)
            num_intermediate_disks = intermediate_disk_counts[0]

            node_ips = map(lambda x: self.coordinator_db.ipv4_address(x), nodes)

            self.ip_address = node_ips[node_id]

            # Get IPs for all interfaces
            node_interface_ips = map(
                lambda x: self.coordinator_db.interfaces(x), nodes)

            intermediate_disks = self.coordinator_db.local_disks(self.hostname)

            # If we're writing output to local disks, we need to know what
            # those local disks are
            output_disks = self.coordinator_db.io_disks(self.hostname)

            # Get the next batch number from the coordinator
            log.info("Waiting for the next batch ...")
            self.current_batch = (
                self.coordinator_db.blocking_wait_for_next_batch(
                    self.hostname))

            log.info("Running batch %d" % (self.current_batch))

            # Make a temporary directory to hold logical disk counts and
            # partition information; put a nonce in the directory name to avoid
            # collisions. Store it on this node's first intermediate disk to
            # avoid running into /tmp size limits

            tmp_files_dir = os.path.join(
                intermediate_disks[0],
                "%(username)s_tempfiles_batch_%(batch_number)d_%(nonce)x" % {
                    "username" : self.username,
                    "batch_number" : self.current_batch,
                    "nonce" : self.batch_nonce })

            assert not os.path.exists(tmp_files_dir)

            os.makedirs(tmp_files_dir)

            # Construct log directory based on current batch
            base_log_dir = create_batch_directory(
                self.log_directory, self.current_batch)

            batch_jobs = self.coordinator_db.batch_jobs(self.current_batch)
            # Determine which phases we're running based on the app config and
            # and first job's job-spec
            job_params = self.coordinator_db.job_params(batch_jobs[0])
            skip_params = [
                "SKIP_PHASE_ZERO", "SKIP_PHASE_ONE", "SKIP_PHASE_TWO",
                "SKIP_PHASE_THREE"]
            skipped_phases = {}
            for param in skip_params:
                # By default don't skip the phase
                skipped_phases[param] = False
                # First load app config
                if param in self.config:
                    skipped_phases[param] = self.config[param]
                # Then load job spec
                if param in job_params:
                    skipped_phases[param] = job_params[param]

            # Special case for daytona minutesort
            daytona_minutesort = False
            if "DAYTONA_MINUTESORT" in job_params and \
                    job_params["DAYTONA_MINUTESORT"]:
                daytona_minutesort = True
                skipped_phases["SKIP_PHASE_ZERO"] = False
                skipped_phases["SKIP_PHASE_ONE"] = True
                skipped_phases["SKIP_PHASE_TWO"] = True
                skipped_phases["SKIP_PHASE_THREE"] = True

            # Need to make a disk-backed boundary list file for each job in the
            # batch, and retrieve any boundary list files for jobs that those
            # jobs are recovering

            global_boundary_list_files = self.lookup_global_boundary_lists(
                batch_jobs, base_log_dir)

            if type(global_boundary_list_files) == int:
                # There was some sort of error while grabbing the boundary
                # file for the returned job; abort this job
                self.fail_current_batch(
                    "Couldn't fetch global boundary list files for job %d" % (
                        global_boundary_list_files))
                self.coordinator_db.node_completed_batch(
                    self.hostname, self.current_batch)
                continue

            # If any part of the batch fails, we should skip all subsequent
            # parts, but still clean up appropriately
            continue_batch = True
            logical_disk_counts_files = {}
            boundary_list_files = {}

            command_params = {
                "OUTPUT_DISK_LIST" : ','.join(output_disks),
                "MYPEERID" : node_id,
                "MY_IP_ADDRESS" : self.ip_address,
                "PEER_LIST" : ','.join(node_interface_ips),
                "NUM_INTERFACES" : self.num_interfaces,
                "CONFIG" : self.config_file,
                "DEFAULT_CONFIG" : self.default_config,
                "SKIP_PHASE_ONE" : 1,
                "SKIP_PHASE_TWO" : 1,
                "SKIP_PHASE_THREE" : 1,
                "COORDINATOR.HOSTNAME" : self.redis_host,
                "COORDINATOR.PORT" : self.redis_port,
                "COORDINATOR.DB" : self.redis_db,
                "BATCH_ID" : str(self.current_batch),
                "NUM_INPUT_DISKS" :
                    len(self.coordinator_db.io_disks(self.hostname))
                }

            if skipped_phases["SKIP_PHASE_ZERO"] == False:
                # Execute phase zero for each job in the batch
                for job_id in batch_jobs:

                    if not continue_batch:
                        break

                    phase_zero_log_dir = os.path.join(
                        base_log_dir, "phase_zero_job_%d" % (job_id))

                    logical_disk_counts_file = os.path.join(
                        tmp_files_dir, "logical_disk_counts.%d" % (job_id))
                    logical_disk_counts_files[job_id] = logical_disk_counts_file

                    boundary_list_file = os.path.join(
                        tmp_files_dir, "boundary_list.%d" % (job_id))
                    boundary_list_files[job_id] = boundary_list_file

                    command_params["LOG_DIR"] = phase_zero_log_dir
                    command_params["LOGICAL_DISK_COUNTS_FILE"] = logical_disk_counts_file
                    command_params["BOUNDARY_LIST_FILE"] = boundary_list_file
                    command_params["JOB_IDS"] = str(job_id)

                    for job_id in global_boundary_list_files:
                        param_name = "DISK_BACKED_BOUNDARY_LIST.%d" % (job_id)

                        command_params[param_name] = (
                            global_boundary_list_files[job_id])

                    if daytona_minutesort:
                        for job_id, filename in boundary_list_files.items():
                            command_params["BOUNDARY_LIST_FILE.%d" % (job_id)] = \
                                filename


                    # Pull in any parameters that may have been set for this job,
                    # overriding the parameters set above
                    for key, value in (
                        self.coordinator_db.job_params(job_id).items()):

                        command_params[key] = value

                    continue_batch = self._run_themis(
                        self.themis_binary, command_params, phase_zero_log_dir)

                    # Copy one of the logical disk counts file to a well-known
                    # location

                    if continue_batch and node_id == 0:
                        if os.path.exists(logical_disk_counts_file):
                            shutil.copy(
                                logical_disk_counts_file,
                                os.path.join(
                                    phase_zero_log_dir,
                                    os.path.basename(logical_disk_counts_file)))
                        else:
                            log.error("Can't find logical disk counts file '%s'"
                                      % (logical_disk_counts_file))

            # Notify redis that we're done with phase zero
            self.coordinator_db.phase_completed(
                self.current_batch, self.ip_address, "phase_zero")

            if skipped_phases["SKIP_PHASE_ONE"] == False:
                # Execute phase one with all jobs at once
                if continue_batch:
                    phase_one_log_dir = os.path.join(base_log_dir, "phase_one")

                    if "BOUNDARY_LIST_FILE" in command_params:
                        del command_params["BOUNDARY_LIST_FILE"]
                    if "LOGICAL_DISK_COUNTS_FILE" in command_params:
                        del command_params["LOGICAL_DISK_COUNTS_FILE"]
                    if "SKIP_PHASE_ONE" in command_params:
                        del command_params["SKIP_PHASE_ONE"]
                    command_params["SKIP_PHASE_ZERO"] = 1
                    command_params["SKIP_PHASE_TWO"] = 1
                    command_params["SKIP_PHASE_THREE"] = 1
                    command_params["JOB_IDS"] = ','.join(map(str, batch_jobs))

                    command_params["LOG_DIR"] = phase_one_log_dir

                    for job_id, filename in logical_disk_counts_files.items():
                        command_params["LOGICAL_DISK_COUNTS_FILE.%d" % (job_id)] = \
                            filename

                    for job_id, filename in boundary_list_files.items():
                        command_params["BOUNDARY_LIST_FILE.%d" % (job_id)] = \
                            filename

                    for job_id in batch_jobs:
                        # Pull in any parameters that may have been set for this job,
                        # overriding the parameters set above
                        # \\\TODO(MC): This doesn't work for multiple jobs.
                        for key, value in (
                            self.coordinator_db.job_params(job_id).items()):

                            command_params[key] = value

                    continue_batch = self._run_themis(
                        self.themis_binary, command_params, phase_one_log_dir)

            # Notify redis that we're done with phase one
            self.coordinator_db.phase_completed(
                self.current_batch, self.ip_address, "phase_one")

            if skipped_phases["SKIP_PHASE_TWO"] == False:
                # Execute phase two with all jobs at once
                if continue_batch:
                    phase_two_log_dir = os.path.join(base_log_dir, "phase_two")

                    if "SKIP_PHASE_TWO" in command_params:
                        del command_params["SKIP_PHASE_TWO"]
                    command_params["SKIP_PHASE_ZERO"] = 1
                    command_params["SKIP_PHASE_ONE"] = 1
                    command_params["SKIP_PHASE_THREE"] = 1
                    command_params["LOG_DIR"] = phase_two_log_dir

                    # Execute phase two
                    continue_batch = self._run_themis(
                        self.themis_binary + "_phase_two", command_params,
                        phase_two_log_dir)

            # Notify redis that we're done with phase two
            self.coordinator_db.phase_completed(
                self.current_batch, self.ip_address, "phase_two")

            if skipped_phases["SKIP_PHASE_THREE"] == False:
                # Execute phase three for each job in the batch
                for job_id in batch_jobs:
                    if not continue_batch:
                        break

                    phase_three_log_dir = os.path.join(
                        base_log_dir, "phase_three_job_%d" % (job_id))

                    if "SKIP_PHASE_THREE" in command_params:
                        del command_params["SKIP_PHASE_THREE"]
                    command_params["SKIP_PHASE_ZERO"] = 1
                    command_params["SKIP_PHASE_ONE"] = 1
                    command_params["SKIP_PHASE_TWO"] = 1
                    command_params["LOG_DIR"] = phase_three_log_dir

                    # Execute phase three
                    continue_batch = self._run_themis(
                        self.themis_binary, command_params, phase_three_log_dir)

            # Notify redis that we're done with phase three
            self.coordinator_db.phase_completed(
                self.current_batch, self.ip_address, "phase_three")

            if continue_batch:
                log.info("Batch %d succeeded" % (self.current_batch))
            else:
                log.info("Batch %d failed" % (self.current_batch))

            # Done processing this batch
            self.coordinator_db.node_completed_batch(
                self.hostname, self.current_batch)

    def fail_current_batch(self, error_msg):
        if self.current_batch is None:
            log.debug("No current batch to clean up")
            return

        # Notify the cluster coordinator that this batch has failed
        self.coordinator_db.report_failure(
            hostname = self.hostname,
            batch_id = self.current_batch,
            message = error_msg)

    def _run_themis(self, binary, command_params, log_dir):
        # Refresh the current set of local disks, which may have changed if
        # disks failed during a previous phase
        intermediate_disks = self.coordinator_db.local_disks(self.hostname)
        assert len(intermediate_disks) > 0

        command_params["INTERMEDIATE_DISK_LIST"] = ','.join(
            intermediate_disks)

        if not os.path.exists(log_dir):
            try:
                os.makedirs(log_dir)
            except:
                # Directory already exists
                pass

        # Start sar, iostat, and vnstat logging.
        # Only run vnstat on the first interface for simplicity.
        interface_list = filter(
            lambda x: len(x) > 0, self.interfaces.split(','))
        monitors = monitor_utils.start_monitors(
            log_dir, self.hostname, interface_list[0])

        # Check core dump settings
        dump_core = False

        themisrc = utils.get_themisrc()

        dump_core = ("dump_core" in themisrc and themisrc["dump_core"])

        params_string = ' '.join(
            map(lambda x: "-%s %s" % (x[0], str(x[1])),
                command_params.items()))

        # If the user specified a profiling tool, run that instead and pass the
        # binary to its first argument.
        if self.profiler is not None:
            profiler_options = ""
            if self.profiler_options is not None:
                profiler_options = self.profiler_options

            if self.profiler == "operf":
                # Use the log directory as the operf session dir
                session_dir = os.path.join(
                    os.path.dirname(log_dir), "oprofile",
                    os.path.basename(log_dir), self.hostname)
                if not os.path.exists(session_dir):
                    os.makedirs(session_dir)
                binary = "%s %s --session-dir=%s %s" % (
                    self.profiler, profiler_options, session_dir, binary)
            else:
                # Some other profiler, just prepend it to the binary
                binary = "%s %s %s" % (self.profiler, profiler_options, binary)

        # If the user specified a library to LD_PRELOAD, set the environment
        # variable before running the binary.
        if self.ld_preload is not None:
            binary = "LD_PRELOAD=%s %s" % (self.ld_preload, binary)

        command = ' '.join((binary, params_string))

        # Create a file containing the command being run
        cmd_log_file = os.path.join(log_dir, "%s.cmd" % (socket.getfqdn()))
        with open(cmd_log_file, 'w') as fp:
            fp.write(command)
            fp.flush()

        core_path = None

        if dump_core:
            # Should be running in the context of one of this host's local
            # disks so that if we dump core, it gets dumped to space that can
            # hold it
            local_disks = self.coordinator_db.local_disks(self.hostname)

            if len(local_disks) > 0:
                run_dir = local_disks[0]
            else:
                run_dir = "/tmp"

            run_dir = os.path.join(run_dir, self.username)

            with open("/proc/sys/kernel/core_pattern", "r") as fp:
                core_filename = fp.read().strip()

            core_path = os.path.abspath(os.path.join(run_dir, core_filename))

            utils.backup_if_exists(core_path)

            if not os.path.exists(run_dir):
                os.makedirs(run_dir)

            command = "cd %s; ulimit -c unlimited; %s" % (run_dir, command)

        stdout_file = os.path.join(log_dir, "stdout-%s.log" % (self.hostname))
        stderr_file = os.path.join(log_dir, "stderr-%s.log" % (self.hostname))

        for filename in [stdout_file, stderr_file]:
            utils.backup_if_exists(filename)

        out_fp = open(stdout_file, 'w')
        err_fp = open(stderr_file, 'w')

        cmd_obj = subprocess.Popen(
            command, shell=True, stdout=out_fp, stderr=err_fp)
        cmd_obj.communicate()

        out_fp.flush()
        out_fp.close()
        err_fp.flush()
        err_fp.close()

        # Terminate sar, iostat, and vnstat
        monitor_utils.stop_monitors(*monitors)

        if cmd_obj.returncode != 0:
            log.error("Themis exited with status %d", cmd_obj.returncode)
            if dump_core:
                assert core_path is not None

                # Identify the core file by its batch number
                if os.path.exists(core_path):
                    core_path_with_batch = os.path.join(
                        os.path.dirname(core_path),
                        "core.batch_%d" % (self.current_batch))
                    shutil.move(core_path, core_path_with_batch)

            with open(stderr_file, 'r') as fp:
                error_msg = fp.read()

            self.fail_current_batch(error_msg)
            log.error(error_msg)

        return cmd_obj.returncode == 0

def main():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("themis_binary", help="path to the Themis binary")
    parser.add_argument("config", help="a YAML file giving configuration "
                        "options for Themis")
    parser.add_argument("log_directory", help="the base log directory where "
                        "the job runner stores its logs")
    parser.add_argument("batch_nonce", help="the nonce for all batches "
                        "executed by this node coordinator", type=int)
    parser.add_argument("--keepalive_refresh", help="the interval, in seconds, "
                        "between refreshes of the key that this node "
                        "coordinator uses to tell the cluster coordinator that "
                        "it's still alive", type=int)
    parser.add_argument("--keepalive_timeout", help="the amount of time that "
                        "must pass without receiving a keepalive message from "
                        "this node coordinator before the cluster coordinator "
                        "considers it to be dead (default: %(default)s "
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

    args.log_directory = create_log_directory(args.log_directory)
    log.info("Logging to %s" % (args.log_directory))

    node_coordinator_log = os.path.join(
        args.log_directory, "node_coordinators",
        "%s.log" % (socket.getfqdn()))

    utils.backup_if_exists(node_coordinator_log)

    logging.basicConfig(
        format="%(levelname)-8s %(asctime)s %(name)-15s %(message)s",
        datefmt="%m-%d %H:%M:%S",
        filename=node_coordinator_log)

    coordinator = None

    def signal_handler(signal_id, frame):
        log.error("Caught signal %s" % (str(signal_id)))
        os.killpg(0, signal.SIGKILL)

        sys.exit(1)

    signal.signal(signal.SIGUSR1, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    try:
        coordinator = NodeCoordinator(**vars(args))
        coordinator.run()
    except:
        # Log and print the exception you just caught
        exception_info = sys.exc_info()

        exception = exception_info[1]

        log.exception(exception)

        traceback.print_exception(*exception_info)

        if (not isinstance(exception, SystemExit)) and coordinator is not None:
            log.error("Marking current batch as failed")
            coordinator.fail_current_batch(
                "Node coordinator error: " + str(exception_info[1]))

    finally:
        if coordinator is not None:
            coordinator.stop_keepalive()

if __name__ == "__main__":
    sys.exit(main())
