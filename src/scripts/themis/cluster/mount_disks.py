#!/usr/bin/env python

import argparse, sys, os, time
from plumbum.cmd import mount, grep, umount, sudo, fdisk, chown, mkdir, head,\
    awk
from plumbum import BG, local

from conf_utils import read_conf_file

mkfsext4 = local["mkfs.ext4"]

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
            if not partitions and "by-id" not in device:
                print "Creating new partition for %s" % device
                (sudo[fdisk[device]] << "d\nn\np\n1\n\n\nw")()

                # It appears that the fdisk command returns before the partition is
                # usable...
                time.sleep(2)

            print "Creating ext4 file system"
            if not partitions and "by-id" not in device:
                # Use partition 1 on the device
                partition = "%s1" % device
            else:
                # The device itself is a partition
                partition = device

            # Persistent devices can use fast formatting
            if "persist" in device:
                extra_opt = "lazy_itable_init=0,lazy_journal_init=0,discard"
                mkfs_commands.append(sudo[mkfsext4]["-F"]["-E"][extra_opt][partition] & BG)
            else:
                mkfs_commands.append(sudo[mkfsext4]["-F"][partition] & BG)

    for command in mkfs_commands:
        command.wait()
        if command.returncode != 0:
            print >>sys.stderr, command.stderr
            sys.exit(command.returncode)

    # Now mount all devices
    disk_index = 0
    persist_disk_index = 0
    for device in devices:
        # Setup mount point
        disk_basename = "disk_persist_%d" % persist_disk_index if "persist" in device else "disk_%d" % disk_index
        disk_mountpoint = os.path.join(mountpoint, disk_basename)
        print "Mounting %s at %s" % (device, disk_mountpoint)
        mkdir["-p"][disk_mountpoint]()
        sudo[chown]["%s:%s" % (username, username)][disk_mountpoint]()

        # Mount disk
        if not partitions and "by-id" not in device:
            # Use partition 1 on the device
            partition = "%s1" % device
        else:
            # The device itself is a partition
            partition = device
        sudo[mount["-o"]["discard,defaults,dioread_nolock,noatime"][partition][disk_mountpoint]]()
        sudo[chown]["%s:%s" % (username, username)][disk_mountpoint]()

        if "persist" in device:
            persist_disk_index += 1
        else:
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
        "--format_disks", action="store_true", help="Format disks")
    parser.add_argument(
        "--partitions", action="store_true", help="If true, assume that the "
        "devices listed in node.conf are partitions and don't run fdisk.")

    args = parser.parse_args()
    return mount_disks(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
