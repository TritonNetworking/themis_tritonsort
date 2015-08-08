#!/usr/bin/env python

import os, sys, argparse, socket, subprocess
import monitor_utils

def run_benchmark_local(vnstat_interface, log_directory, themis_binary):
    hostname = socket.gethostname()

    stdout_file = os.path.join(log_directory, "stdout.%s.log" % hostname)
    stderr_file = os.path.join(log_directory, "stderr.%s.log" % hostname)

    out_fp = open(stdout_file, 'w')
    err_fp = open(stderr_file, 'w')

    # Start system monitoring processes
    monitors = monitor_utils.start_monitors(
        log_directory, hostname, vnstat_interface)

    # Launch themis
    command = subprocess.Popen(
        themis_binary, shell=True, stdout=out_fp, stderr=err_fp)
    command.communicate()

    out_fp.flush()
    out_fp.close()
    err_fp.flush()
    err_fp.close()

    # Terminate sar, iostat, and vnstat
    monitor_utils.stop_monitors(*monitors)

def main():
    parser = argparse.ArgumentParser(
        description="Run a benchmark application on a single node.")
    parser.add_argument("--vnstat_interface", default=None, help="interface on"
                        "which vnstat will measure network traffic")
    parser.add_argument("log_directory", help="directory where logs are stored")
    parser.add_argument("themis_binary", help="themis binary string with "
                        "parameters")

    args = parser.parse_args()
    run_benchmark_local(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
