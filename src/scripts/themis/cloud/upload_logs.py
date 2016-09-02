#!/usr/bin/env python

import sys, os

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
sys.path.append(os.path.abspath(os.path.join(
    SCRIPT_DIR, os.pardir, "cluster")))

from conf_utils import read_conf_file

from plumbum import local
parallel_ssh = local[
    os.path.join(
        SCRIPT_DIR, os.pardir, "cluster", "parallel_ssh.py")]

from plumbum.commands.processes import ProcessExecutionError

def upload_logs():
    cluster_ID, log_directory, provider = read_conf_file(
        "cluster.conf", "cluster", ["id", "log_directory", "provider"])

    if provider == "amazon":
        S3_bucket = read_conf_file("amazon.conf", "amazon", "bucket")
        parallel_ssh["-m"]["aws --profile themis s3 sync --exact-timestamps "\
                           "%s s3://%s/cluster_%s/themis_logs" % (
                               log_directory, S3_bucket, cluster_ID)]()
    elif provider == "google":
        bucket = read_conf_file("google.conf", "google", "bucket")

        done = False
        while not done:
            try:
                parallel_ssh["-m"]["gsutil -m rsync -r %s gs://%s/cluster_%s/" \
                                   "themis_logs" % (
                                       log_directory, bucket, cluster_ID)]()
                done = True
            except ProcessExecutionError as e:
                pass
    else:
        print >>sys.stderr, "Unknown provider %s" % provider
        return 1

    return 0

def main():
    return upload_logs()

if __name__ == "__main__":
    sys.exit(main())
