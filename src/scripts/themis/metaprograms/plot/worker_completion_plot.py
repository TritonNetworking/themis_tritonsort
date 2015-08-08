#!/usr/bin/env python

import os, sys, argparse, utils, pprint

from boomslang import Plot, PlotLayout, Bar, StackedBars

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, os.pardir, os.pardir)))

from descriptions.descriptionutils import Description
import themis.metaprograms.utils.utils as log_utils
from themis.metaprograms.utils import StatQuery

def handle_worker_runtime(stat, data):
    hostname = stat["hostname"]
    job = stat["job_name"]
    phase = stat["phase_name"]
    stage = stat["stage_name"]
    worker_id = stat["id"]
    start_time = stat["start_time"]
    stop_time = stat["stop_time"]

    data_key = [(job, phase)]
    data_subdict = log_utils.populate_nested_dictionary(data, data_key)

    if "min_timestamp" not in data_subdict:
        data_subdict["min_timestamp"] = start_time

    data_subdict["min_timestamp"] = min(
        data_subdict["min_timestamp"], start_time)

    if "max_timestamp" not in data_subdict:
        data_subdict["max_timestamp"] = stop_time

    data_subdict["max_timestamp"] = max(
        data_subdict["max_timestamp"], stop_time)

    data_subdict[(hostname, stage, worker_id)] = (start_time, stop_time)

class Duration(object):
    def __init__(self, hostname, stage, worker_id, start_time, stop_time):
        __slots__ = ("hostname", "stage", "worker_id", "start_time",
                     "stop_time")

        self.hostname = hostname
        self.stage = stage
        self.worker_id = worker_id
        self.start_time = start_time
        self.stop_time = stop_time

def plot_timeline_for_phase(log_directory, job, phase, phase_data):
    min_timestamp = phase_data["min_timestamp"]
    max_timestamp = phase_data["max_timestamp"]

    description = Description(os.path.join(log_directory, job, "description"))
    stage_ordering = description.getStageOrdering(phase)

    duration_lists = {}

    for stage in stage_ordering:
        duration_lists[stage] = []

    for key in phase_data:
        if key in ["min_timestamp", "max_timestamp"]:
            continue

        hostname, stage, worker_id = key

        worker_duration_info = Duration(
            hostname.split('.')[0], stage, worker_id,
            (phase_data[key][0] - min_timestamp) / 1000000.0,
            (phase_data[key][1] - min_timestamp) / 1000000.0)

        duration_lists[stage].append(worker_duration_info)

    def sort_function(x):
        return (x.hostname, x.worker_id, x.start_time, x.stop_time)

    layout = PlotLayout()

    for stage in stage_ordering:
        duration_list = duration_lists[stage]

        duration_list.sort(key=sort_function)

        bars = {}

        # Set up a "padding" bar that will appear to move bars up so that they
        # start when the worker starts
        start_bar = Bar()
        start_bar.linewidth = 0
        start_bar.color = "white"

        for i, duration in enumerate(duration_list):
            if duration.hostname not in bars:
                bars[duration.hostname] = Bar()

            bars[duration.hostname].yValues.append(
                duration.stop_time - duration.start_time)
            start_bar.yValues.append(duration.start_time)

        # Make sure that all bars have the same number of y-axis values,
        # give them x-axis values and set their colors

        start_bar.xValues = range(len(start_bar.yValues))
        start_bar.xTickLabelProperties = {
            "rotation" : 90
            }

        bar_colors = ["red", "blue", "green", "orange", "gray", "pink",
                      "purple", "black"]

        offset = 0

        for i, (hostname, bar) in enumerate(bars.items()):
            # Pad y axis with zeroes so that bars can be laid out next to
            # each other with a StackedBars
            num_y_values = len(bar.yValues)

            bar.yValues = (([0] * offset) + bar.yValues +
                           ([0] *
                            (len(duration_list) - (num_y_values + offset))))

            # Put the label for this hostname roughly in the middle of its bar
            # cluster
            start_bar.xTickLabels.append(hostname)
            # Subtracting 0.5 to account for half the width of the bar
            start_bar.xTickLabelPoints.append(offset + (num_y_values / 2.0)
                                              - 0.5)

            offset += num_y_values

            bar.xValues = range(len(bar.yValues))
            bar.color = bar_colors[i % len(bar_colors)]
            bar.label = hostname

        stacked_bars = StackedBars()
        stacked_bars.add(start_bar)

        for hostname in sorted(bars.keys()):
            stacked_bars.add(bars[hostname])

        plot = Plot()
        plot.setYLimits(0, ((max_timestamp - min_timestamp) / 1000000.0) * 1.05)
        plot.setXLabel("Worker")
        plot.setYLabel("Time (s)")
        plot.setTitle(stage)

        plot.add(stacked_bars)
        layout.addPlot(plot)

    return layout

def worker_completion_plot(log_directory, verbose):
    queries = []

    # Get each runtime for each worker in the cluster
    query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "worker_runtime"),
        ("start_time", None))
    query.match_processor_function = handle_worker_runtime
    queries.append(query)

    worker_runtime_data = log_utils.process_queries(
        queries, log_directory, verbose)

    for (job, phase) in worker_runtime_data:
        phase_data = worker_runtime_data[(job, phase)]
        plot = plot_timeline_for_phase(
            log_directory, job, phase, phase_data)
        plot.dpi = 250
        print "Saving plot for job %s phase %s" % (job, phase)
        plot.save("%s_%s_completion_plot.png" % (job, phase))

    return 0

def main():
    parser = argparse.ArgumentParser(
        description="display worker runtimes as bars")
    parser.add_argument("log_directory", help="a directory containing logs "
                        "for an experiment")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()

    return worker_completion_plot(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
