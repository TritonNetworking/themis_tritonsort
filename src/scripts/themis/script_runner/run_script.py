#!/usr/bin/env python

import os, sys, argparse, getpass, redis, time, signal, random, paramiko

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

import utils as themis_utils

def run_script(redis_host, redis_port, redis_db, command, user, hosts):
    pending_commands = []
    start_time = time.time()
    try:
        command = ' '.join(command)

        if hosts is None:
            # User did not specify host list override, so ask redis
            redis_client = redis.StrictRedis(
                host=redis_host, port=redis_port, db=redis_db)

            hosts = redis_client.smembers("nodes")

            if hosts is None:
                print >>sys.stderr, "Error extracting host list from redis database"
                return 1
        else:
            # User did specify host list as a comma-delimited list, so parse it
            hosts = filter(lambda x: len(x) > 0, hosts.split(','))

        temp_dir = "/tmp/run-script-%s-%s-%08x" % (
            getpass.getuser(), time.strftime("%Y-%m-%d-%H%M.%S"),
            random.randint(0, (16 ** 8) - 1))

        if os.path.exists(temp_dir):
            print >>sys.stderr, (
                "Temporary directory %s already (and extremely improbably) "
                "exists; aborting" % (temp_dir))
            return 1

        os.makedirs(temp_dir)

        hosts_file = os.path.join(temp_dir, "hosts")

        with open(hosts_file, 'w') as fp:
            fp.write('\n'.join(hosts) + '\n')

        stderr_dir = os.path.join(temp_dir, "stderr")
        stdout_dir = os.path.join(temp_dir, "stdout")

        for dirname in stderr_dir, stdout_dir:
            os.makedirs(dirname, 0755)

        for host in hosts:
            ssh_client = paramiko.SSHClient()
            ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            ssh_client.connect(host, username=user)
            channel = ssh_client.get_transport().open_session()
            channel.get_pty()
            channel.exec_command(command)

            pending_commands.append((host, ssh_client, channel))
            print "Launching remote command on %s (%d / %d)" % (
                host, len(pending_commands), len(hosts))

        host_failed = False

        while len(pending_commands) > 0:
            for host, ssh_client, channel in pending_commands:
                # Loop until we find a command that finished.
                if channel.exit_status_ready():
                    num_completed_nodes = len(hosts) - len(pending_commands)
                    # This node just completed.
                    num_completed_nodes += 1
                    elapsed_time = time.time() - start_time
                    print "%s completed remote command in %.2f seconds " \
                        "(%d / %d)" % (
                        host, elapsed_time, num_completed_nodes, len(hosts))

                    return_code = channel.recv_exit_status()
                    if return_code != 0:
                        print >>sys.stderr, "%s FAILED:" % (host)
                        host_failed = True

                    # Save stdout and stderr to file
                    stdout_file = os.path.join(stdout_dir, host)
                    with open(stdout_file, "w") as fp:
                        while channel.recv_ready():
                            stdout = channel.recv(1024)
                            fp.write(stdout)

                    stderr_file = os.path.join(stderr_dir, host)
                    with open(stderr_file, "w") as fp:
                        while channel.recv_stderr_ready():
                            stderr = channel.recv_stderr(1024)
                            fp.write(stderr)
                            if return_code != 0:
                                sys.stderr.write(stderr)

                    ssh_client.close()
                    pending_commands.remove((host, ssh_client, channel))
                    break
            time.sleep(0.1)

        pending_commands = []

        if host_failed:
            return 1
        else:
            return 0
    except KeyboardInterrupt:
        print >>sys.stderr, "\nCaught keyboard interrupt\n"
        return 1
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
    parser = argparse.ArgumentParser(description="run a script on all nodes")
    parser.add_argument(
        "--user", help="the username as whom this job will be run "
        "(default: %(default)s)", default=getpass.getuser())
    parser.add_argument(
        "--hosts", help="a comma-delimited list of hosts to use instead of "
        "contacting redis")
    parser.add_argument(
        "command", help="the command to be run", nargs=argparse.REMAINDER)

    themis_utils.add_redis_params(parser)

    args = parser.parse_args()
    return run_script(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
