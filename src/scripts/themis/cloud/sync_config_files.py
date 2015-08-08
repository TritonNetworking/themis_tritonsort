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

def sync_config_files():
    cluster_ID, config_directory, provider = read_conf_file(
        "cluster.conf", "cluster", ["id", "config_directory", "provider"])

    if provider == "amazon":
        S3_bucket = read_conf_file("amazon.conf", "amazon", "bucket")
        aws = local["aws"]

        # First upload local config files
        aws["--profile"]["themis"]["s3"]["sync"]["--exact-timestamps"]\
            [os.path.expanduser(config_directory)]\
            ["s3://%s/cluster_%s/themis_config" % (S3_bucket, cluster_ID)]()

        # Then download config files to all nodes
        parallel_ssh["-m"]["aws --profile themis s3 sync --exact-timestamps "\
                               "s3://%s/cluster_%s/themis_config %s" % (
                S3_bucket, cluster_ID, config_directory)]()
    elif provider == "google":
        bucket = read_conf_file("google.conf", "google", "bucket")
        gsutil = local["gsutil"]

        # First upload local config files
        gsutil["-m"]["cp"]["-r"]\
            [os.path.expanduser(config_directory)]\
            ["gs://%s/cluster_%s" % (bucket, cluster_ID)]()

        # Then download config files to all nodes

        # gsutil appears to be buggy and can fail randomly so keep trying until
        # you succeed. Try 5 times even if it succeeds to make sure all files
        # get synced.
        for i in xrange(5):
            done = False
            while not done:
                try:
                    parallel_ssh["-m"]["gsutil -m rsync -r -c gs://%s/cluster_%s/" \
                                       "themis_config %s" % (
                                           bucket, cluster_ID, config_directory)]()
                    done = True
                except ProcessExecutionError as e:
                    pass

    else:
        print >>sys.stderr, "Unknown provider %s" % provider
        return 1

    return 0

def main():
    return sync_config_files()

if __name__ == "__main__":
    sys.exit(main())
