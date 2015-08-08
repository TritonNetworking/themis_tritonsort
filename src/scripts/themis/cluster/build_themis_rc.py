#!/usr/bin/env python

import sys, os, argparse

from conf_utils import read_conf_file

THEMIS_RC = os.path.expanduser("~/.themisrc")

def build_themis_rc(dump_core, port, database):
    port = int(port)

    # Read the cluster config to get master address.
    master, keyfile = read_conf_file(
        "cluster.conf", "cluster", ["master_internal_address", "private_key"])
    keyfile = os.path.join(os.path.expanduser("~"), ".ssh", keyfile)

    if os.path.exists(THEMIS_RC):
        os.unlink(THEMIS_RC)

    # .themisrc is written in yaml so do this manually
    with open(THEMIS_RC, "w") as themisrc_file:
        themisrc_file.write("ssh:\n")
        themisrc_file.write("  key: \"%s\"\n\n" % keyfile)

        if dump_core:
            themisrc_file.write("dump_core: true\n\n")
        else:
            themisrc_file.write("dump_core: false\n\n")

        themisrc_file.write("redis:\n")
        themisrc_file.write("  host: \"%s\"\n" % master)
        themisrc_file.write("  port: %d\n" % port)
        themisrc_file.write("  db: %d\n\n" % database)

    return 0

def main():
    parser = argparse.ArgumentParser(
        description="Build themisrc file")

    parser.add_argument(
        "--dump_core", action="store_true",
        help="If set, core files will be dumped on failure")
    parser.add_argument(
        "--port", type=int, default=6379,
        help="Port to use for redis. Default %(default)s")
    parser.add_argument(
        "--database", type=int, default=0,
        help="Database to use for redis. Default %(default)s")

    args = parser.parse_args()
    return build_themis_rc(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
