#!/usr/bin/env python

import sys

from string import Template
import subprocess
import logging

log = logging.getLogger("dfs")


def checked_filesystem_call(operation):
    cmd_obj = subprocess.Popen(operation, shell=True,
                               stdout = subprocess.PIPE,
                               stderr = subprocess.STDOUT)

    output = cmd_obj.communicate()[0]
    status = cmd_obj.returncode

    return (operation, filter(lambda x: len(x) > 0, output.split('\n')),
            status == 0)

def serial_filesystem_operation(operation, disks, ignore_failed_disks = False):
    template = Template(operation)

    ops = [template.safe_substitute(disk = d) for d in disks]

    results = [checked_filesystem_call(op) for op in ops]

    if ignore_failed_disks == False:
        num_failed_disks = 0

        for (operation, output) in (x[0:2] for x in results if x[2] == False):
            log.error("Command '%s' failed: %s" % (operation, output))
            num_failed_disks += 1

        if num_failed_disks > 0:
            return None

    return [output[1] for output in results if output[2] == True]
