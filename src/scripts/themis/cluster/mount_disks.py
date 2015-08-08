#!/usr/bin/env python

import argparse, sys, os, time
from plumbum.cmd import mount, grep, umount, sudo, fdisk, chown, mkdir, head,\
    awk
from plumbum import BG, local

from conf_utils import read_conf_file

mkfsxfs = local["mkfs.xfs"]

def mount_disks(format_disks, mountpoint, partitions):
    # Get comma delimited list of devices
    devices = read_conf_file("node.conf", "node", "devices")
    devices = devices.split(",")
    devices = [d for d in devices if len(d) > 0]

    username = read_conf_file("cluster.conf", "cluster", "username")

    # Setup mount point
    sudo[mkdir["-p"][mountpoint]]()
    sudo[chown]["%s:%s" % (username, username)][mountpoint]()

    mkfs_commands = []
    for device in devices:
        # Unmount ALL partitions connected to this device
        num_mounted = (mount | grep["-c"][device])(retcode=(0,1))
        num_mounted = int(num_mounted.strip())

        while num_mounted > 0:
            # Unmount device
            mounted_device =\
                (mount | grep[device] | head["-n1"] | awk["{print $1}"])()
            mounted_device = mounted_device.strip()

            print "Unmounting %s" % mounted_device
            sudo[umount[mounted_device]]()

            num_mounted -= 1

        # Format device
        if format_disks:
            if not partitions:
                print "Creating new partition for %s" % device
                (sudo[fdisk[device]] << "d\nn\np\n1\n\n\nw")()

                # It appears that the fdisk command returns before the partition is
                # usable...
                time.sleep(2)

            print "Creating xfs file system"
            if not partitions:
                # Use partition 1 on the device
                partition = "%s1" % device
            else:
                # The device itself is a partition
                partition = device

            mkfs_commands.append(sudo[mkfsxfs]["-f"][partition] & BG)

    for command in mkfs_commands:
        command.wait()
        if command.returncode != 0:
            print >>sys.stderr, command.stderr
            sys.exit(command.returncode)

    # Now mount all devices
    disk_index = 0
    for device in devices:
        # Setup mount point
        disk_mountpoint = os.path.join(mountpoint, "disk_%d" % disk_index)
        print "Mounting %s at %s" % (device, disk_mountpoint)
        mkdir["-p"][disk_mountpoint]()
        sudo[chown]["%s:%s" % (username, username)][disk_mountpoint]()

        # Mount disk
        if not partitions:
            # Use partition 1 on the device
            partition = "%s1" % device
        else:
            # The device itself is a partition
            partition = device
        sudo[mount["-o"]["noatime,discard"][partition][disk_mountpoint]]()
        sudo[chown]["%s:%s" % (username, username)][disk_mountpoint]()

        disk_index += 1

def main():
    parser = argparse.ArgumentParser(
        description="Mount Themis disks")

    disk_mountpoint = read_conf_file(
        "cluster.conf", "cluster", "disk_mountpoint")

    parser.add_argument(
        "--mountpoint", default=disk_mountpoint,
        help="Mount point for disks. Default %(default)s")
    parser.add_argument(
        "--format_disks", action="store_true", help="Format disks with XFS")
    parser.add_argument(
        "--partitions", action="store_true", help="If true, assume that the "
        "devices listed in node.conf are partitions and don't run fdisk.")

    args = parser.parse_args()
    return mount_disks(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
