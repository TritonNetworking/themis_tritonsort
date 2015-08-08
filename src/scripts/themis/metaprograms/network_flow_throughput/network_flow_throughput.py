#!/usr/bin/env python

import sys, argparse, re

connection_time_regex = re.compile(r"^DATM\t.*sender\t(\d+)\tconnection_time_(\d+)\t\d+\t\d+\t(\d+)")
bytes_sent_regex = re.compile(r"^DATM\t.*sender\t(\d+)\ttotal_bytes_sent_(\d+)\t(\d+)")
num_peers_regex = re.compile(r"^DATM\t.*sender\t(\d+)\tnum_peers\t(\d+)")

def build_nested_dict(data_dict, sender, flow):
    if sender not in data_dict:
        data_dict[sender] = {}
    if flow not in data_dict[sender]:
        data_dict[sender][flow] = {"sec": None, "MB": None}

    return data_dict

def get_peer_data(log_file):
    flow_data = {}
    num_peers_data = {}

    with open(log_file, "r") as fp:
        for line in fp:
            line = line.strip()
            if num_peers_regex.match(line) is not None:
                matched_groups = num_peers_regex.match(line)
                sender = int(matched_groups.group(1))
                num_peers = int(matched_groups.group(2))

                num_peers_data[sender] = num_peers

            if connection_time_regex.match(line) is not None:
                matched_groups = connection_time_regex.match(line)
                sender = int(matched_groups.group(1))
                flow = int(matched_groups.group(2))
                connection_time = int(matched_groups.group(3))
                # Convert to seconds
                connection_time /= 1000000.0

                flow_data = build_nested_dict(flow_data, sender, flow)
                flow_data[sender][flow]["sec"] = connection_time

            elif bytes_sent_regex.match(line) is not None:
                matched_groups = bytes_sent_regex.match(line)
                sender = int(matched_groups.group(1))
                flow = int(matched_groups.group(2))
                bytes_sent = int(matched_groups.group(3))
                # Convert to MB
                MB_sent = bytes_sent / 1000000.0

                flow_data = build_nested_dict(flow_data, sender, flow)
                flow_data[sender][flow]["MB"] = MB_sent

    peer_flow_data = {}

    # Break data out into per-peer flows
    for sender in flow_data:
        peer_flow_data[sender] = {}
        num_peers = num_peers_data[sender]
        for peer in xrange(num_peers):
            peer_flow_data[sender][peer] = {}

        for flow in flow_data[sender]:
            peer = flow % num_peers
            peer_flow_data[sender][peer][flow] = flow_data[sender][flow]

    # Compute peer throughput totals
    peer_total_bytes = {}
    peer_max_times = {}
    for sender in peer_flow_data:
        for peer in peer_flow_data[sender]:
            if peer not in peer_total_bytes:
                peer_total_bytes[peer] = 0.0
                peer_max_times[peer] = 0.0

            for flow in peer_flow_data[sender][peer]:
                peer_total_bytes[peer] += \
                    peer_flow_data[sender][peer][flow]["MB"]
                peer_max_times[peer] = \
                    max(peer_max_times[peer],
                        peer_flow_data[sender][peer][flow]["sec"])

    return (peer_flow_data, peer_max_times, peer_total_bytes)

def network_flow_throughput(log_file):
    (peer_flow_data, peer_max_times, peer_total_bytes) = get_peer_data(log_file)

    # Display data.
    for sender in peer_flow_data:
        print "Sender %d:" % sender
        for peer in peer_flow_data[sender]:
            print "  Peer %d:" % peer
            for flow in peer_flow_data[sender][peer]:
                data = peer_flow_data[sender][peer][flow]
                throughput = data["MB"] / data["sec"]
                print "    Flow %d: %.2f MB/s (%.2f MB over %.2f seconds)" % (
                    flow, throughput, data["MB"], data["sec"])

    print ""
    print "Total peer throughputs:"
    for peer in peer_total_bytes:
        peer_throughput = peer_total_bytes[peer] / peer_max_times[peer]
        print "  Peer %d: %.2f MB/s (%.2f MB over %.2f seconds)" % (
            peer, peer_throughput, peer_total_bytes[peer], peer_max_times[peer])

    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Analyze network flow throughputs")
    parser.add_argument("log_file", help="a log stat file to analyze")

    args = parser.parse_args()

    return network_flow_throughput(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
