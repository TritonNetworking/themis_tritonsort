#!/usr/bin/env python

import os, sys, argparse, re, numpy

started_regex = re.compile(r"^\[STATUS\] Started .*\((\d+)\)")
closed_regex = re.compile(r"^\[STATUS\] sender (\d+) closing connection (\d+) to peer (\d+)\((\d+)\)")

def sender_completion_times(log_file):
    start_time = None

    completion_time_dict = {}
    completed_connections = []

    with open(log_file, "r") as fp:
        for line in fp:
            line = line.strip()
            if started_regex.match(line) is not None:
                matched_groups = started_regex.match(line)
                start_time = int(matched_groups.group(1))
            elif closed_regex.match(line) is not None:
                matched_groups = closed_regex.match(line)
                sender = int(matched_groups.group(1))
                connection = int(matched_groups.group(2))
                peer = int(matched_groups.group(3))
                closed_time = int(matched_groups.group(4))

                if sender not in completion_time_dict:
                    completion_time_dict[sender] = {}
                if peer not in completion_time_dict[sender]:
                    completion_time_dict[sender][peer] = {}

                # Convert to seconds
                completion_time = (closed_time - start_time) / 1000000.0
                completion_time_dict[sender][peer][connection] = completion_time

                # Store as a tuple so we can rank order connections
                completed_connections.append(
                    (completion_time, sender, peer, connection))

    # Print all completion times
    print "Sender completion times:"
    for sender in completion_time_dict:
        for peer in completion_time_dict[sender]:
            for connection in completion_time_dict[sender][peer]:
                completion_time = completion_time_dict[sender][peer][connection]
                print "Sender %d, Peer %d, Connection %d: %f" % (
                    sender, peer, connection, completion_time)

    # Now sort the 4-tuples and print in ascending order.
    completed_connections = sorted(
        completed_connections, key=lambda connection_tuple: connection_tuple[0])

    print ""
    print "Rank ordered completion times:"
    for index, (completion_time, sender, peer, connection) in \
            enumerate(completed_connections):
        print "#%d: Sender %d, Peer %d, Connection %d: %f" % (
            index, sender, peer, connection, completion_time)

    print ""
    print "Completion time statistics"
    # Get some basic statistics about the completion times, including aggregate
    # mean, median and per-peer mean/median
    num_peers = len(completion_time_dict[0])

    completion_times = [x[0] for x in completed_connections]
    print "Overall mean completion time: %f" % numpy.mean(completion_times)
    print "Overall median completion time: %f" % numpy.median(completion_times)

    # Get per peer statistics
    for peer in xrange(num_peers):
        completion_times = [x[0] for x in completed_connections if x[2] == peer]
        print "Receiving peer %d mean completion time: %f" % (
            peer, numpy.mean(completion_times))
        print "Receiving peer %d median completion time: %f" % (
            peer, numpy.median(completion_times))

def main():
    parser = argparse.ArgumentParser(
        description="display sender socket completion times")
    parser.add_argument("log_file", help="a log file to check for completion "
                        "times")

    args = parser.parse_args()

    return sender_completion_times(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
