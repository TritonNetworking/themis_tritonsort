#!/usr/bin/env python

import argparse
import json
import os
from boomslang import Line, Plot, PlotLayout, Scatter

class LineAndScatter:
    """
    LineAndScatter is a utility object that encapsulates both a Line and a
    Scatter. Points are added to the line unless the y value is None, in which
    case they are added to the scatter with a y value of -1.0.  The idea is that
    the scatter represents invalid data points. In the case of the benchmark,
    the scatter will contain experiments that failed because the scratch size
    was larger than RAM.
    """
    def __init__(self, label, color):
        line = Line()
        line.marker = "."
        line.label = label
        line.color = color
        self.line = line

        scatter = Scatter()
        scatter.marker = "x"
        scatter.color = color
        scatter.markerSize = 200
        self.scatter = scatter

    def add_point(self, x, y):
        assert x is not None, "Cannot add point (%s, %s) with None x value." % (
            x, y)
        if y is not None:
            # There is a y value, so add the point to the line.
            self.line.xValues.append(x)
            self.line.yValues.append(y)
        else:
            # There is not a y value. Represent the point as a scatter plot X at
            # -1.0
            self.scatter.xValues.append(x)
            self.scatter.yValues.append(-1.0)

    def add_to_plot(self, plot):
        # Always add the line
        plot.add(self.line)
        # Only add the scatter if it's non-empty
        if len(self.scatter.xValues) > 0:
            plot.add(self.scatter)


def median(data):
    data.sort()

    if len(data) == 0:
        return None
    else:
        return data[len(data) / 2]

def plotExperiments(script_arguments_file):
    script_arguments_fp = open(script_arguments_file, "r")
    script_arguments = json.load(script_arguments_fp)
    script_arguments_fp.close()

    disk = script_arguments["disk"]

    sort_strategies = script_arguments["sort_strategies"]
    max_key_lengths = map(int, script_arguments["max_key_lengths"])
    max_value_lengths = map(int, script_arguments["max_value_lengths"])
    pareto_as = map(float, script_arguments["pareto_as"])
    pareto_bs = map(int, script_arguments["pareto_bs"])
    byte_counts = map(int, script_arguments["byte_counts"])
    timeout = script_arguments["timeout"]

    experiments = [(sort_strategy, max_key_length, max_value_length, pareto_a,
                    pareto_b, byte_count)
                   for sort_strategy in sort_strategies
                   for max_key_length in max_key_lengths
                   for max_value_length in max_value_lengths
                   for pareto_a in pareto_as
                   for pareto_b in pareto_bs
                   for byte_count in byte_counts]

    # Open each file and store information about the experiment
    file_id = 0
    experiment_outcomes = {}
    for experiment in experiments:
        filename = os.path.join(disk, "stats_%d.json" % file_id)
        sort_time = None
        scratch_size = None
        if os.path.exists(filename):
            fp = open(filename, "r")
            try:
                stats_json = json.load(fp)
                # time might not be present if the run timed out
                if "time" in stats_json:
                    # sort time in seconds
                    sort_time = stats_json["time"] / 1000000.0
                # scratch size in megabytes
                scratch_size = stats_json["scratch_size"] / 1000000.0
            except ValueError:
                # If the JSON somehow doesn't parse, just skip this file
                pass
        experiment_outcomes[experiment] = (sort_time, scratch_size)
        file_id += 1

    # Specify colors manually so we can sync up a line to its scatter
    colors = ["red", "green", "blue", "teal", "orange"]
    if len(sort_strategies) > len(colors):
        print >> sys.stderr, "Only %d colors but %d lines" % (
            len(colors), len(sort_strategies))
        sys.exit(1)

    # Plot sort time vs scratch size
    plot = Plot()
    plot.setXLabel("Scratch Size (MB)")
    plot.setYLabel("Sort Time (s)")
    plot.hasLegend(labelSize = 8, columns=3)
    plot.setTitle("Sort Time vs Scratch Size")

    lines = {}
    for (sort_strategy, color) in zip(sort_strategies, colors):
        line = LineAndScatter(sort_strategy, color)
        lines[sort_strategy] = line

    for experiment, (sort_time, scratch_size) in \
            experiment_outcomes.iteritems():
        # Mark this experiment on the line corresponding to its sort strategy
        line = lines[experiment[0]]
        if scratch_size is not None:
            line.add_point(scratch_size, sort_time)

    map(lambda l: l.add_to_plot(plot), lines.values())

    layout = PlotLayout()
    layout.addPlot(plot)
    output_file = os.path.join(disk, "SortTimeVsScratchSize.pdf")
    layout.save(output_file)

    # Plot Sort time vs Scratch Size of a particular strategy:
    # First sort the experiments by sort strategy
    strategy_outcomes = {}
    for experiment in experiments:
        sort_strategy = experiment[0]
        parameters = experiment[1:]
        if parameters not in strategy_outcomes:
            strategy_outcomes[parameters] = {}
        strategy_outcomes[parameters][sort_strategy] = \
            experiment_outcomes[experiment]

    # Create one plot per strategy, with that strategy's scratch size
    # on the x-axis
    layout = PlotLayout()
    for major_sort_strategy in sort_strategies:
        # Set up plot boilerplate
        plot = Plot()
        plot.setXLabel("Scratch Size of %s (MB)" % (major_sort_strategy))
        plot.setYLabel("Sort Time (s)")
        plot.hasLegend(labelSize = 8, columns=3)
        plot.setTitle("Sort Time vs Scratch Size of %s" % (major_sort_strategy))

        lines = {}
        for (sort_strategy, color) in zip(sort_strategies, colors):
            line = LineAndScatter(sort_strategy, color)
            lines[sort_strategy] = line
        # For each configuration, plot the sort times on the y axis, and the
        # scratch time for the major_sort_strategy on the x-axis
        for outcome_by_strategy in strategy_outcomes.values():
            scratch_size = outcome_by_strategy[major_sort_strategy][1]
            if scratch_size is not None:
                # Plot the sort time of each strategy using this scratch
                # As the x-axis value.
                for sort_strategy in sort_strategies:
                    sort_time = outcome_by_strategy[sort_strategy][0]
                    lines[sort_strategy].add_point(scratch_size, sort_time)

        map(lambda l: l.add_to_plot(plot), lines.values())

        layout.addPlot(plot)
    output_file = os.path.join(disk, "SortTimeVsScratchSizeByStrategy.pdf")
    layout.save(output_file)

    # Plot individual parameter graphs using median parameters
    median_max_key_length = median(max_key_lengths)
    median_max_value_length = median(max_value_lengths)
    median_pareto_a = median(pareto_as)
    median_pareto_b = median(pareto_bs)
    median_byte_count = median(byte_counts)

    for (parameter_name, x_label, parameter_values, fixed_parameters) in [
        ("MaxKeyLength", "Max Key Length (bytes)", max_key_lengths,
         (None, median_max_value_length, median_pareto_a, median_pareto_b,
          median_byte_count)),
        ("MaxValueLength", "Max Value Length (bytes)", max_value_lengths,
         (median_max_key_length, None, median_pareto_a, median_pareto_b,
          median_byte_count)),
        ("ParetoA", "Pareto A", pareto_as,
         (median_max_key_length, median_max_value_length, None,
          median_pareto_b, median_byte_count)),
        ("ParetoB", "Pareto B (bytes)", pareto_bs,
         (median_max_key_length, median_max_value_length, median_pareto_a, None,
          median_byte_count)),
        ("FileSize", "File Size (bytes)", byte_counts,
         (median_max_key_length, median_max_value_length, median_pareto_a,
          median_pareto_b, None))]:

        time_plot = Plot()
        size_plot = Plot()
        for (plot, label, unit) in [(time_plot, "Sort Time", "s"),
                                    (size_plot, "Scratch Size", "MB")]:
            plot.setXLabel(x_label)
            plot.setYLabel("%s (%s)" % (label, unit))
            plot.hasLegend(labelSize = 8, columns=3)
            plot.setTitle("%s vs %s" % (label, parameter_name))

        for (sort_strategy, color) in zip(sort_strategies, colors):
            # Each sort strategy has its own line on each of the two plots
            time_line = LineAndScatter(sort_strategy, color)
            size_line = LineAndScatter(sort_strategy, color)

            # Compute the set of parameters corresponding to this experiment.
            for parameter in parameter_values:
                # Apply this parameter to the None value in the tuple of
                # fixed parameters.
                parameters = [sort_strategy]
                for fixed_parameter in fixed_parameters:
                    if fixed_parameter is None:
                        parameters.append(parameter)
                    else:
                        parameters.append(fixed_parameter)

                # Plot sort time and scratch size for this experiment
                (sort_time, scratch_size) = experiment_outcomes[
                    tuple(parameters)]

                time_line.add_point(parameter, sort_time)
                size_line.add_point(parameter, scratch_size)

            # Add lines to plots
            time_line.add_to_plot(time_plot)
            size_line.add_to_plot(size_plot)

        # Save the plot as a pdf
        layout = PlotLayout()
        layout.addPlot(time_plot)
        layout.addPlot(size_plot)
        output_file = os.path.join(disk, "%s.pdf" % (parameter_name))
        layout.save(output_file)

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="plot results of the sort strategy benchmark")

    parser.add_argument(
        "script_arguments_file",
        help="script arguments file generated by runAllExperiments.py")

    args = parser.parse_args()

    plotExperiments(**vars(args))
