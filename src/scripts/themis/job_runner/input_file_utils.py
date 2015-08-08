import os, sys, string, subprocess, requests, logging, json, itertools, random
from urlparse import urlparse

logging.basicConfig(
    format="%(levelname)-8s %(asctime)s %(name)-15s %(message)s",
    datefmt="%m-%d %H:%M:%S")

log = logging.getLogger("gather_files")
log.setLevel(logging.DEBUG)

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

def generate_read_request(job_ids, file_url, offset, length):
    read_request = {
        "type" : 0,
        "job_ids" : job_ids,
        "path" : file_url,
        "offset" : offset,
        "length" : length
        }

    return read_request

def generate_halt_request(job_ids):
    halt_request = {
        "type" : 1,
        "job_ids" : job_ids
        }

    return halt_request

def hdfs_file_paths_to_worker_inputs(
    coordinator_db, hdfs_host, hdfs_port, hdfs_files):

    worker_inputs = {}

    # Snapshot live nodes, so that we don't have the set of live nodes change
    # on us in the middle of divvying out work

    live_nodes = list(coordinator_db.live_nodes)

    round_robin_index = 0

    for (filename, file_length, proxied) in hdfs_files:
        if proxied:
            # Figure out what node and disk the primary replica is on, and
            # let the appropriate worker service it
            path_chunks = filter(lambda x: len(x) > 0, filename.split('/'))
            host_ip = path_chunks[0]
            hostname = coordinator_db.hostname(host_ip)

            assert hostname is not None

            if hostname not in live_nodes:
                # Assign files to other hosts in round-robin order
                reassigned_hostname = live_nodes[round_robin_index]
                round_robin_index = (round_robin_index + 1) % len(live_nodes)

                log.error(("Re-assigning '%s' from dead node '%s' to live node "
                           "'%s'") % (filename, hostname, reassigned_hostname))

                hostname = reassigned_hostname

            host_disks = coordinator_db.hdfs_disks(hostname)

            target_disk = None

            try:
                if int(path_chunks[1]) in xrange(len(host_disks)):
                    target_disk = int(path_chunks[1])
            except ValueError:
                log.error("Target disk '%s' falls outside disk list "
                          "boundaries (disk list had %d elements)" % (
                        path_chunks[1], len(host_disks)))
                target_disk = None

            assert target_disk is not None

            if hostname not in worker_inputs:
                worker_inputs[hostname] = {}

            if target_disk not in worker_inputs[hostname]:
                worker_inputs[hostname][target_disk] = []

            file_url = "hdfs://%s:%d%s" % (hdfs_host, hdfs_port, filename)

            worker_inputs[hostname][target_disk].append(
                (file_url, file_length))

        else:
            # TODO: Push this filename onto a list of files to be assigned
            # to workers round-robin
            log.error("Coordinator doesn't support unproxied inputs yet "
                      "(unproxied input: %s)" % (filename))


    return worker_inputs

def gather_input_file_paths(
    coordinator_db, input_url, max_input_files_per_disk = None):
    # Parse input directory as a URL
    parse_result = urlparse(input_url)

    input_dir = parse_result.path

    url_scheme = parse_result.scheme

    if url_scheme == "":
        log.info("Defaulting to 'local' URL type for input URL '%s'" %
                 (input_url))
        url_scheme = "local"

    if url_scheme == "local":
        return gather_local_file_paths(
            coordinator_db, input_dir, max_input_files_per_disk)

    elif url_scheme == "hdfs":
        if parse_result.netloc.find(':') != -1:
            host, port = parse_result.netloc.split(':')
            port = int(port)
        else:
            host = parse_result.netloc
            port = 50070

        # Get a list of files from HDFS
        hdfs_files_info = gather_hdfs_file_paths(
            host, port, input_dir)

        if hdfs_files_info is None:
            log.error("Failed to gather a list of input files for '%s'" %
                      (input_dir))
            return None

        (files, total_input_size) = hdfs_files_info

        # Convert those files into a map of the following form:
        # map[hostname][disk_number] = [(url, length), (url, length), ...]
        worker_inputs = hdfs_file_paths_to_worker_inputs(
            coordinator_db, host, port, files)

        return (worker_inputs, total_input_size)
    else:
        log.error("Unknown protocol '%s' for input URL '%s'" %
                  (url_scheme, input_url))
        return None

def gather_local_file_paths(
    coordinator_db, input_dir, max_input_files_per_disk):
    ssh_command = utils.ssh_command()

    hosts = coordinator_db.live_nodes

    ssh_command_template = string.Template(
        "%(ssh_command)s ${host} '%(script_path)s/list_local_files.py ${disks}'"
        % {
            "ssh_command" : ssh_command,
            "script_path" : os.path.abspath(os.path.dirname(__file__))
            })

    pending_commands = []

    for host in hosts:
        disks = list(coordinator_db.io_disks(host))
        disks.sort()

        local_dirs = map(lambda x: "%s/%s" % (x, input_dir), disks)

        cmd = ssh_command_template.substitute(
            host=host,
            disks = ' '.join(local_dirs))

        pending_commands.append(
            (host, subprocess.Popen(cmd, shell="True", stdout=subprocess.PIPE)))

    worker_inputs = {}
    total_input_size = 0

    for hostname, command in pending_commands:
        worker_inputs[hostname] = {}

        stdout, stderr = command.communicate()

        if command.returncode != 0:
            log.error("Command '%s' failed with error %d" % (
                    command.cmd, command.returncode))
            return None

        file_paths = json.loads(stdout)

        if file_paths is None:
            log.error(("Input directory '%s' doesn't exist on all of host %s's "
                       "input disks") % (input_dir, hostname))
            return None

        for i, file_list in enumerate(file_paths):
            worker_inputs[hostname][i] = []

            num_files = 0
            for filename, file_length in file_list:
                # Allow the user to cap the number of input files for testing.
                if max_input_files_per_disk == None or \
                        num_files < max_input_files_per_disk:
                    file_url = "local://%s%s" % (hostname, filename)
                    worker_inputs[hostname][i].append((file_url, file_length))
                    total_input_size += file_length
                    num_files += 1

    return (worker_inputs, total_input_size)

def gather_hdfs_file_paths(host, port, input_dir):
    try:
        response = requests.get(
            "http://%s:%d/webhdfs/v1%s?op=LISTSTATUS" % (
                host, port, input_dir),
            config={"trust_env" : False})
    except requests.exceptions.ConnectionError, e:
        log.exception(e)
        return None

    if response.status_code != 200:
        try:
            exception_message = (
                json.loads(response.content)["RemoteException"]["message"])
            log.error("LISTSTATUS on '%s' failed with error %d: %s" % (
                    input_dir, response.status_code, exception_message))
        except ValueError:
            log.error("LISTSTATUS on '%s' failed with error %d: %s" % (
                    input_dir, response.status_code, response.content))
        finally:
            return None

    directory_listing = (
        json.loads(response.content)["FileStatuses"]["FileStatus"])

    input_file_paths = []

    input_size = 0

    for file_info in directory_listing:
        if file_info["type"] == "FILE":
            if "proxyPath" in file_info:
                file_path = "%s/%s" % (
                    file_info["proxyPath"], file_info["pathSuffix"])
                proxied = True
            else:
                file_path = "%s/%s" % (input_dir, file_info["pathSuffix"])
                proxied = False

            file_length = file_info["length"]

            input_file_paths.append((file_path, file_length, proxied))
            input_size += file_length
        else:
            dir_path = "%s/%s" % (input_dir, file_info["pathSuffix"])

            (paths_in_dir, dir_input_size) = gather_hdfs_file_paths(
                host, port, dir_path)

            input_size += dir_input_size

            if paths_in_dir == None:
                log.error("Unable to get contents of '%s'" % (
                        dir_path))
                return None
            else:
                input_file_paths.extend(paths_in_dir)

    return (input_file_paths, input_size)

def generate_read_requests(
    job_inputs, phase_zero_sample_rate, phase_zero_sample_points_per_file,
    tuple_start_offset, job_ids, phases = list([0, 1])):

    assert phase_zero_sample_rate <= 1.0,\
        "Cannot have a sample rate greater than 1. Got %f" % (
        phase_zero_sample_rate)

    scan_shared_inputs = utils.NestedDict(3, list)

    for (job_id, worker_inputs) in itertools.izip(job_ids, job_inputs):
        for host, worker in utils.flattened_keys(worker_inputs):
            for file_info in worker_inputs[host][worker]:
                scan_shared_inputs[host][worker][file_info].append(job_id)

    read_requests = utils.NestedDict(2, list)
    phase_one_read_requests = utils.NestedDict(2, list)

    for phase in sorted(phases):
        if phase == 0:
            # Read file prefixes for phase zero sampling for each job
            for job_id in job_ids:
                for host, worker, file_info in utils.flattened_keys(
                    scan_shared_inputs):

                    file_url, file_length = file_info
                    # Compute the number of sampled bytes from the sample rate
                    sample_length = file_length * phase_zero_sample_rate

                    # Compute the number of bytes per sample point from the
                    # number of sample points within a file
                    if phase_zero_sample_points_per_file > 1:
                        assert tuple_start_offset != 0, "Cannot sample " \
                            "multiple points per file without specifying a " \
                            "tuple start offset."
                    sample_length_per_sample_point = \
                        sample_length / phase_zero_sample_points_per_file
                    sample_point_offset = \
                        file_length / phase_zero_sample_points_per_file

                    # If we know tuple boundary offsets, then force whole tuples
                    if tuple_start_offset > 0:
                        sample_length_per_sample_point -= \
                            sample_length_per_sample_point % tuple_start_offset
                        sample_point_offset -= \
                            sample_point_offset % tuple_start_offset

                    # At this point we are guaranteed that
                    #  sample length <= sample offset
                    #  both are multiples of the tuple length if fixed size
                    #    tuples

                    for i in xrange(phase_zero_sample_points_per_file):
                        # Chunk up the sample data into fixed size samples
                        # spread evenly across the file
                        read_requests[host][worker].append(
                            generate_read_request(
                                [job_id], file_url, i * sample_point_offset,
                                sample_length_per_sample_point))

                # Add a halt request after all of the samples for this worker
                for host, worker in utils.flattened_keys(read_requests):
                    read_requests[host][worker].append(generate_halt_request(
                            [job_id]))

        elif phase == 1:
            for host, worker, file_info in utils.flattened_keys(
                scan_shared_inputs):

                file_url, file_length = file_info
                file_jobs = scan_shared_inputs[host][worker][file_info]

                phase_one_read_requests[host][worker].append(
                    generate_read_request(file_jobs, file_url, 0, file_length))

            for host, worker in utils.flattened_keys(phase_one_read_requests):
                # Randomly permute input files in phase one.
                requests = list(phase_one_read_requests[host][worker])
                random.shuffle(requests)
                for request in requests:
                    read_requests[host][worker].append(request)

                read_requests[host][worker].append(generate_halt_request(
                        job_ids))

    return read_requests

def load_read_requests(coordinator_db, read_requests):
    # Load read requests into redis

    for hostname in read_requests:
        host_ip = coordinator_db.ipv4_address(hostname)

        for worker_id in read_requests[hostname]:
            coordinator_db.add_read_requests(
                host_ip, worker_id, *(read_requests[hostname][worker_id]))
