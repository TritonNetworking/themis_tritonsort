#!/usr/bin/env python

import sys, argparse, os

from network_flow_throughput import get_peer_data

def find_slow_receivers(log_directory, percentile):
    log_files = [
        x for x in os.listdir(log_directory) if x.find("stats.log") != -1]
    flow_times = []
    for log_file in log_files:
        (peer_flow_data, peer_max_times, peer_total_bytes) = \
            get_peer_data(os.path.join(log_directory, log_file))

        for sender in peer_flow_data:
            for peer in peer_flow_data[sender]:
                for flow in peer_flow_data[sender][peer]:
                    flow_time = peer_flow_data[sender][peer][flow]["sec"]
                    flow_times.append((flow_time, log_file, sender, peer, flow))

    flow_times.sort(key=lambda t: t[0], reverse=True)

    # Display data.
    num_flows = len(flow_times)
    num_flows_to_show = int(num_flows * (percentile / 100.0))
    for (flow_time, log_file, sender, peer, flow) in \
            flow_times[:num_flows_to_show]:
        print "%.2fs - Peer %d - Sender %d Flow %d from %s" % (
            flow_time, peer, sender, flow, log_file)
    return 0


def main():
    parser = argparse.ArgumentParser(
        description="Find slow receivers in an experiment.")
    parser.add_argument("log_directory", help="a directory containing stat "
                        "files to analyze")
    parser.add_argument("-p", "--percentile", type=int, default=100,
                        help="Show only the slowest percentile flow times")

    args = parser.parse_args()

    return find_slow_receivers(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
