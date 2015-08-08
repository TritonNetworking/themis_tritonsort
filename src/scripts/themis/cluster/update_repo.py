#!/usr/bin/env python

import sys, os, argparse

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))
UPDATE_REPO = os.path.join(SCRIPT_DIR, "update_repo.sh")

from plumbum import local
parallel_ssh = local[os.path.join(SCRIPT_DIR, "parallel_ssh.py")]

def update_repo(branch, recompile, debug, asserts):
    recompile_string = "N"
    if recompile:
        recompile_string = "Y"
    debug_string = "N"
    if debug:
        debug_string = "Y"
    asserts_string = "N"
    if asserts:
        asserts_string = "Y"

    parallel_ssh["-m"]["%s %s %s %s %s" % (
        UPDATE_REPO, branch, recompile_string, debug_string, asserts_string)]()

    return 0

def main():
    parser = argparse.ArgumentParser(description="Update Themis repository")

    parser.add_argument(
        "--branch", "-b", default="master",
        help="Branch to checkout. Default %(default)s")

    parser.add_argument(
        "--recompile", "-r", action="store_true",
        help="Recompile themis after updating.")
    parser.add_argument(
        "--debug", "-d", action="store_true",
        help="If recompiling, compile with debug symbols")
    parser.add_argument(
        "--asserts", "-a", action="store_true",
        help="If recompiling, compile with asserts")

    args = parser.parse_args()
    return update_repo(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
