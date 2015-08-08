#!/usr/bin/env python

import os, sys, argparse, getpass, redis, time, signal, random, paramiko,\
    socket

from conf_utils import read_conf_file

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

import utils as themis_utils

def parallel_ssh(
        redis_client, command, username, hosts, ignore_bad_hosts, master,
        verbose=True):

    pending_commands = []
    stdout_dict = {}
    stderr_dict = {}

    start_time = time.time()
    try:
        if type(command) == list:
            command = ' '.join(command)

        if command == "":
            print >>sys.stderr, "Cannot run empty command."
            return (1, stdout_dict, stderr_dict)

        if verbose:
            print "Running %s in parallel." % command

        if hosts is None:
            # User did not specify host list override, so ask redis
            hosts = redis_client.smembers("nodes")

            if hosts is None:
                print >>sys.stderr, "Error extracting host list from "\
                    "redis database"
                return (1, stdout_dict, stderr_dict)
        else:
            hosts = set(hosts)

        if master:
            # Also run on the master node.
            master_address = read_conf_file(
                "cluster.conf", "cluster", "master_internal_address")

            if verbose:
                print "Including master %s" % master_address
            hosts.add(master_address)

        temp_dir = "/tmp/run-script-%s-%s-%08x" % (
            username, time.strftime("%Y-%m-%d-%H%M.%S"),
            random.randint(0, (16 ** 8) - 1))

        if os.path.exists(temp_dir):
            print >>sys.stderr, (
                "Temporary directory %s already (and extremely improbably) "
                "exists; aborting" % (temp_dir))
            return (1, stdout_dict, stderr_dict)

        os.makedirs(temp_dir)

        hosts_file = os.path.join(temp_dir, "hosts")

        with open(hosts_file, 'w') as fp:
            fp.write('\n'.join(hosts) + '\n')

        stderr_dir = os.path.join(temp_dir, "stderr")
        stdout_dir = os.path.join(temp_dir, "stdout")

        for dirname in stderr_dir, stdout_dir:
            os.makedirs(dirname, 0755)

        for host in hosts:
            try:
                ssh_client = paramiko.SSHClient()
                ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                ssh_client.connect(host, username=username)
                channel = ssh_client.get_transport().open_session()
                channel.get_pty()
                channel.exec_command(command)

                pending_commands.append((host, ssh_client, channel))
                if verbose:
                    print "Launching remote command on %s (%d / %d)" % (
                        host, len(pending_commands), len(hosts))
            except socket.gaierror as error:
                if not ignore_bad_hosts:
                    raise error

        host_failed = False

        for host in hosts:
            stdout_dict[host] = ""
            stderr_dict[host] = ""

        while len(pending_commands) > 0:
            for host, ssh_client, channel in pending_commands:
                # Loop until we find a command that finished.
                if channel.exit_status_ready():
                    num_completed_nodes = len(hosts) - len(pending_commands)
                    # This node just completed.
                    num_completed_nodes += 1
                    elapsed_time = time.time() - start_time
                    if verbose:
                        print "%s completed remote command in %.2f seconds " \
                            "(%d / %d)" % (
                            host, elapsed_time, num_completed_nodes, len(hosts))

                    return_code = channel.recv_exit_status()
                    if return_code != 0:
                        print >>sys.stderr, "%s FAILED:" % (host)
                        host_failed = True

                    # Save stdout and stderr to file and to dicts
                    stdout_file = os.path.join(stdout_dir, host)
                    with open(stdout_file, "w") as fp:
                        while channel.recv_ready():
                            stdout = channel.recv(1024)
                            fp.write(stdout)
                            stdout_dict[host] += stdout

                    stderr_file = os.path.join(stderr_dir, host)
                    with open(stderr_file, "w") as fp:
                        while channel.recv_stderr_ready():
                            stderr = channel.recv_stderr(1024)
                            fp.write(stderr)
                            stderr_dict[host] += stderr
                            if return_code != 0 and verbose:
                                sys.stderr.write(stderr)

                    ssh_client.close()
                    pending_commands.remove((host, ssh_client, channel))
                    break
            time.sleep(0.1)

        pending_commands = []

        if host_failed:
            return (1, stdout_dict, stderr_dict)
        else:
            return (0, stdout_dict, stderr_dict)
    except KeyboardInterrupt:
        print >>sys.stderr, "\nCaught keyboard interrupt\n"
        return  (1, stdout_dict, stderr_dict)
    finally:
        # Cleanly stop any pending commands
        remaining_hosts = len(pending_commands)
        if remaining_hosts > 0:
            for host, ssh_client, channel in pending_commands:
                print >>sys.stderr, (
                    "Killing pending command on host '%s' ..." % (host))

                ssh_client.close()

            elapsed_time = time.time() - start_time
            print "Remaining %d commands terminated at %.2f seconds." % (
                remaining_hosts, elapsed_time)
            pending_commands = []

def main():
    parser = argparse.ArgumentParser(
        description="Run a command or script in over ssh in parallel across "
        "all nodes in the cluster")
    parser.add_argument(
        "--user", help="the username to run under "
        "(default: %(default)s)", default=getpass.getuser())
    parser.add_argument(
        "--hosts", help="a comma-delimited list of hosts to use instead of "
        "contacting redis")
    parser.add_argument(
        "--ignore_bad_hosts", help="if set, ignore hosts that couldn't be "
        "reached, rather than failing", action="store_true")
    parser.add_argument(
        "--master", "-m", help="if set, also run command on master node",
        action="store_true")
    parser.add_argument(
        "command", help="the command to be run", nargs="+")

    themis_utils.add_redis_params(parser)

    args = parser.parse_args()
    username = args.user
    hosts = args.hosts
    if hosts != None:
        # Separate comma-delimited list
        hosts = filter(lambda x: len(x) > 0, hosts.split(','))

    redis_client = redis.StrictRedis(
        host=args.redis_host, port=args.redis_port, db=args.redis_db)

    return_code, _, _ = parallel_ssh(
        redis_client, args.command, username, hosts, args.ignore_bad_hosts,
        args.master)
    return return_code

if __name__ == "__main__":
    sys.exit(main())
