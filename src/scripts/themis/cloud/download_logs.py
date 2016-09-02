#!/usr/bin/env python

import sys, os, plumbum

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.join(
    SCRIPT_DIR, os.pardir, "cluster")))

from conf_utils import read_conf_file

from plumbum.commands.processes import ProcessExecutionError

def download_logs():
    cluster_ID, log_directory, provider = read_conf_file(
        "cluster.conf", "cluster", ["id", "log_directory", "provider"])
    log_directory = os.path.expanduser(log_directory)

    if provider == "amazon":
        S3_bucket = read_conf_file("amazon.conf", "amazon", "bucket")
        aws = plumbum.local["aws"]
        aws["--profile"]["themis"]["s3"]["sync"]["--exact-timestamps"]\
            ["s3://%s/cluster_%s/themis_logs" % (S3_bucket, cluster_ID)]\
            [log_directory]()
    elif provider == "google":
        bucket = read_conf_file("google.conf", "google", "bucket")

        done = False
        gsutil = plumbum.local["gsutil"]
        while not done:
            try:
                gsutil["-m"]["rsync"]["-r"]\
                    ["gs://%s/cluster_%s/themis_logs" % (bucket, cluster_ID)]\
                    [log_directory]()
                done = True
            except ProcessExecutionError as e:
                pass

    else:
        print >>sys.stderr, "Unknown provider %s" % provider
        return 1

    return 0

def main():
    return download_logs()

if __name__ == "__main__":
    sys.exit(main())
