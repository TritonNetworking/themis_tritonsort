import redis, json, itertools

def sorted_list(original_function):
    """
    A decorator that outputs anything this function outputs as a sorted list
    """
    def sl_function(*args, **kwargs):
        struct = original_function(*args, **kwargs)

        if struct is None:
            return None
        else:
            l = list(struct)
            l.sort()
            return l

    return sl_function

class CoordinatorDB(object):
    """
    Wrapper around a Redis client that abstracts the actual Redis operations
    needed to interact with the cluster coordinator's database
    """
    def __init__(self, host, port, db):
        self.redis_client = redis.StrictRedis(host=host, port=port, db=db)

    # Information about the nodes

    @sorted_list
    def hdfs_disks(self, hostname):
        return self.redis_client.smembers("node_hdfs_disks:%s" % (hostname))

    @sorted_list
    def local_disks(self, hostname):
        return self.redis_client.sdiff(
            "node_local_disks:%s" % (hostname),
            "failed_local_disks:%s" % (hostname))

    @sorted_list
    def failed_local_disks(self, hostname):
        return self.redis_client.smembers("failed_local_disks:%s" % (hostname))

    @sorted_list
    def io_disks(self, hostname):
        return self.redis_client.smembers("node_io_disks:%s" % (hostname))

    def mark_local_disk_failed(self, hostname, failed_disk):
        self.redis_client.sadd(
            "failed_local_disks:%s" % (hostname), failed_disk)

    def hostname(self, ipv4_address):
        return self.redis_client.hget("hostname", ipv4_address)

    def ipv4_address(self, hostname):
        return self.redis_client.hget("ipv4_address", hostname)

    def interfaces(self, hostname):
        return self.redis_client.hget("interfaces", hostname)

    @property
    def num_interfaces(self):
        return int(self.redis_client.get("num_interfaces"))

    @property
    def known_nodes(self):
        return set(self.redis_client.smembers("nodes"))

    @property
    def live_nodes(self):
        return self.redis_client.sdiff("nodes", "dead_nodes")

    @property
    def dead_nodes(self):
        return self.redis_client.smembers("dead_nodes")

    @property
    def incomplete_batches(self):
        return map(int, self.redis_client.smembers("incomplete_batches"))

    def node_coordinator_pid(self, hostname):
        return int(self.redis_client.get(self.liveness_key(hostname)))

    def update_pid(self, hostname, pid):
        self.redis_client.set("keepalive:%s" % (hostname), pid)

    def refresh_keepalive(self, hostname, timeout):
        self.redis_client.expire("keepalive:%s" % (hostname), timeout)

    def batch_jobs(self, batch_id):
        return map(
            int, self.redis_client.lrange("batch_jobs:%d" % (batch_id), 0, -1))

    # Creating/updating job info

    def new_job_info(self, job_id, job_description):
        job_info_key = "job_info:%d" % (job_id)

        assert not self.redis_client.exists(job_info_key)

        # Create a hash for this job in redis that the job issuer can poll for
        # changes in job status
        job_info = {
            "job_id" : "%d" % (job_id),
            "status" : "In Progress",
            "fail_message" : "",
            "input_directory" : job_description["input_directory"],
            "intermediate_directory" : \
                job_description["intermediate_directory"],
            "output_directory" : job_description["output_directory"],
            "map_function" : job_description["map_function"],
            "reduce_function" : job_description["reduce_function"],
            "partition_function" : job_description["partition_function"],
            "job_title" : job_description["job_title"],
            "total_input_size_bytes" : "Unknown"
            }

        self.redis_client.hmset(job_info_key, job_info)

        # If the job has specified parameters, store those parameters
        if "params" in job_description:
            self.redis_client.hmset(
                "job_params:%d" % (job_id), job_description["params"])

    def update_job_status(
        self, job_id, field_changes, pre_status=None, post_status=None):

        job_info_key = "job_info:%s" % (job_id)

        # Transactionally apply field_changes and change 'status' field to
        # post_status only if 'status' field is currnetly pre_status

        pipe = self.redis_client.pipeline()

        set_status = False

        while not set_status:
            try:
                pipe.watch(job_info_key)

                status = pipe.hget(job_info_key, "status")

                if pre_status is None or status == pre_status:
                    pipe.multi()

                    if post_status is not None:
                        pipe.hset(job_info_key, "status", post_status)

                    pipe.hmset(job_info_key, field_changes)
                    pipe.execute()
                else:
                    pipe.reset()

                set_status = True

            except redis.WatchError:
                continue

    def job_info(self, job_id):
        return self.redis_client.hgetall("job_info:%d" % (job_id))

    @property
    def job_names_and_ids(self):
        job_names_to_ids = self.redis_client.hgetall("coordinator_job_id")

        for key in job_names_to_ids:
            job_names_to_ids[key] = int(job_names_to_ids[key])

        return job_names_to_ids

    def job_params(self, job_id):
        job_params = {}

        job_params_key = "job_params:%d" % (job_id)

        if self.redis_client.exists(job_params_key):
            job_params = self.redis_client.hgetall(job_params_key)

        if self.redis_client.exists("hdfs:namenode"):
            hdfs_host, hdfs_port = self.redis_client.hmget(
                "hdfs:namenode", ["hostname", "port"])
            job_params["HDFS.HOSTNAME"] = hdfs_host
            job_params["HDFS.PORT"] = hdfs_port

        return job_params

    # Recovery information

    def setup_recovery_job(self, job_id, recovering_job):
        recovery_info = {
            "recovering_job" : int(recovering_job)
            }

        self.redis_client.hmset("recovery_info:%d" % (job_id), recovery_info)

    def recovery_info(self, job_id):
        recovery_info_key = "recovery_info:%d" % (job_id)

        if self.redis_client.exists(recovery_info_key):
            recovery_info = self.redis_client.hgetall(recovery_info_key)

            recovery_info["recovering_job"] = int(
                recovery_info["recovering_job"])

            return recovery_info
        else:
            return None

    def global_boundary_list_file(self, job_id):
        return self.redis_client.hget("disk_backed_boundary_lists", str(job_id))

    def set_global_boundary_list_file(self, job_id, path):
        self.redis_client.hset("disk_backed_boundary_lists", str(job_id), path)

    # Updating the state of a batch

    def mark_batch_complete(self, batch_id):
        self.redis_client.srem("incomplete_batches", batch_id)
        self.redis_client.delete("batch_remaining:%d" % (batch_id))

    def mark_batch_incomplete(self, batch_id):
        self.redis_client.sadd("incomplete_batches", batch_id)
        self.redis_client.sadd(
            "batch_remaining:%d" % (batch_id), *(self.live_nodes))

    def node_completed_batch(self, hostname, batch_id):
        self.redis_client.srem(
            "batch_remaining:%d" % (batch_id), hostname)

    # Update the state of a host

    def declare_host_dead(self, hostname):
        self.redis_client.sadd("dead_nodes", hostname)

    def declare_host_alive(self, hostname):
        self.redis_client.srem("dead_nodes", hostname)

    # Node coordinator keepalives

    def liveness_key(self, hostname):
        return "keepalive:%s" % (hostname)

    def create_keepalive(self, hostname):
        self.redis_client.set(self.liveness_key(hostname), "-1")

    def keepalive_refreshed(self, hostname):
        return self.redis_client.exists(self.liveness_key(hostname))

    def batch_failed(self, batch_id):
        return self.redis_client.sismember("failed_batches", batch_id)

    def fail_batch(self, batch_id):
        self.redis_client.sadd("failed_batches", batch_id)

    # Managing failure report queue

    def report_failure(self, hostname, batch_id, message, disk = None):
        failure_report = {
            "hostname" : hostname,
            "batch_id" : batch_id,
            "disk" : disk,
            "message" : message
            }

        self.redis_client.rpush(
            "node_failure_reports", json.dumps(failure_report))

    def next_failure_report(self):
        failure_report = self.redis_client.lpop("node_failure_reports")

        if failure_report is None:
            return None

        try:
            failure_report = json.loads(failure_report)
        except:
            return dict()

        # It's possible that the client reported a path on one of the disks,
        # rather than the root of that disk's mountpoint. We should strip that
        # extra information off before returning the report

        if failure_report["disk"] is None:
            return failure_report

        local_disks = self.local_disks(failure_report["hostname"])

        failed_disk_path = failure_report["disk"]

        for disk in local_disks:
            if failed_disk_path.find(disk) == 0:
                failure_report["disk"] = disk

                return failure_report

        return dict()

    # Managing job queue

    def next_job(self):
        job_descriptions_with_ids = []

        job_description_str = self.redis_client.lpop("job_queue")

        if job_description_str != None:
            # Set up the new job and add it to the current batch
            job_descriptions = json.loads(job_description_str)

            if type(job_descriptions) != list:
                job_descriptions = [job_descriptions]

            for job_description in job_descriptions:
                # Search for a job ID for the new job
                job_id = int(self.redis_client.incr("next_job_id"))

                # Associate job name with job ID
                self.redis_client.hset(
                    "coordinator_job_id", job_description["job_name"],
                    "%d" % (job_id))

                job_descriptions_with_ids.append((job_description, job_id))

        return job_descriptions_with_ids


    def clear_job_queue(self):
        self.redis_client.delete("job_queue")

    @property
    def next_batch_id(self):
        batch_id = self.redis_client.incr("next_batch_id")

        # Delete any stale job listings for this batch that might be lying
        # around
        self.redis_client.delete("batch_jobs:%s" % (batch_id))

        return batch_id

    def add_jobs_to_batch(self, batch_id, batch_jobs):
        self.redis_client.rpush(
            "batch_jobs:%s" % (batch_id), *map(str, batch_jobs))

    def clear_batch_queue(self, hostname):
        self.redis_client.delete("batch_queue:%s" % (hostname))

    def add_batch_to_node_coordinator_batch_queues(self, batch_id):
        for host in self.live_nodes:
            self.redis_client.rpush("batch_queue:%s" % (host), batch_id)

    def blocking_wait_for_next_batch(self, hostname):
        batch_info = self.redis_client.blpop("batch_queue:%s" % (hostname))

        assert batch_info is not None

        if batch_info is not None:
            # Extract the current batch from the 2-tuple returned by blpop,
            # ignoring the name of the queue from which we popped
            return int(batch_info[1])

    def remaining_nodes_running_batch(self, batch_id):
        return self.redis_client.scard("batch_remaining:%s" % (batch_id))

    def send_ping_request(self, hostname):
        self.redis_client.rpush("ping_request:%s" % (hostname), "go")

    def wait_for_ping_request(self, hostname):
        self.redis_client.blpop("ping_request:%s" % (hostname))

    def send_ping_reply(self, hostname, reply):
        self.redis_client.rpush("ping_reply:%s" % (hostname), reply)

    def wait_for_ping_reply(self, hostname):
        return self.redis_client.blpop("ping_reply:%s" % (hostname))[1]

    def add_recovery_partition_range(self, job_id, range_start, range_stop):
        range_str = "%s-%s" % (range_start, range_stop)

        self.redis_client.sadd(
            "recovering_partitions:%d" % (int(job_id)), range_str)

    def add_read_requests(self, host_ip, worker_id, *read_requests):
        host_read_req_queues_key = "read_requests:%s" % (host_ip)

        read_request_queue_key = "read_requests:%s:reader:%d" % (
            host_ip, worker_id)

        # Make sure the node's set of read request queues includes
        # this one if it doesn't already

        # TODO(AR) This could cause a lot of unnecessary sadd calls, but I'd
        # rather not prematurely optimize this
        self.redis_client.sadd(host_read_req_queues_key, read_request_queue_key)

        self.redis_client.rpush(
            read_request_queue_key, *itertools.imap(json.dumps, read_requests))

    def begin_phase(self, batch_id, phase_name):
        nodes = self.live_nodes
        ips = map(lambda x: self.ipv4_address(x), nodes)
        key = "running_nodes:batch_%s:%s" % (batch_id, phase_name)
        self.redis_client.sadd(key, *(ips))
        # Expire the running nodes list after 1 week in case of failure so
        # don't clutter the database with old lists.
        self.redis_client.expire(key, 604800)

    def phase_completed(self, batch_id, node_ip, phase_name):
        # Some node completed a phase of computation, so record it.
        self.redis_client.rpush(
            "%s_completed_nodes:batch_%d" % (phase_name, batch_id), node_ip)
        self.redis_client.srem(
            "running_nodes:batch_%s:%s" % (batch_id, phase_name), node_ip)

    def completed_node_for_phase(self, batch_id, phase_name):
        # Get a node that completed this phase, if any.
        return self.redis_client.lpop(
            "%s_completed_nodes:batch_%d" % (phase_name, batch_id))

    def query_running_nodes(self, batch_id, phase_name):
        return self.redis_client.smembers(
            "running_nodes:batch_%s:%s" % (batch_id, phase_name))

    def create_barriers(self, phases, batch_id, batch_jobs):
        # Create a barriers for each phase and fill them with the set of live
        # nodes.
        barriers = ["phase_start", "sockets_connected"]
        phase_names = ["phase_zero", "phase_one", "phase_two", "phase_three"]
        nodes = self.live_nodes
        ips = map(lambda x: self.ipv4_address(x), nodes)
        for barrier_name in barriers:
            for phase in phases:
                if phase == 0 or phase == 3:
                    # Phase 0 and 3 require one barrier per job.
                    for job_id in batch_jobs:
                        key = "barrier:%s:%s:%d:%d" % (
                            barrier_name, phase_names[phase], batch_id, job_id)
                        self.redis_client.sadd(key, *(ips))
                        # Expire the barrier after 1 week in case of failure so
                        # don't clutter the database with old barriers.
                        self.redis_client.expire(key, 604800)
                else:
                    # Phase 1 and 2 require a single barrier for the batch. Use
                    # the job index of 0 to simplify the barrier logic
                    key = "barrier:%s:%s:%d:0" % (
                        barrier_name, phase_names[phase], batch_id)
                    self.redis_client.sadd(key, *(ips))
                    # Expire the barrier after 1 week in case of failure so
                    # don't clutter the database with old barriers.
                    self.redis_client.expire(key, 604800)

    def query_barrier(self, phase_name, batch_id):
        barriers = ["phase_start", "sockets_connected"]
        batch_jobs = self.batch_jobs(batch_id)
        if phase_name == "phase_zero" or phase_name == "phase_three":
            for job_id in batch_jobs:
                for barrier_name in barriers:
                    running_nodes = list(self.redis_client.smembers(
                            "barrier:%s:%s:%d:%d" % (
                                barrier_name, phase_name, batch_id, job_id)))
                    if len(running_nodes) > 0:
                        return (barrier_name, running_nodes, job_id)
        elif phase_name == "phase_one" or phase_name == "phase_two":
            for barrier_name in barriers:
                running_nodes = list(self.redis_client.smembers(
                        "barrier:%s:%s:%d:0" % (
                            barrier_name, phase_name, batch_id)))
                if len(running_nodes) > 0:
                    return (barrier_name, running_nodes, None)

        return (None, None, None)


