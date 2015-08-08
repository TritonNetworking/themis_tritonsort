#!/usr/bin/env python

import os, sys, argparse, warnings, itertools

from boomslang import Line, Plot, PlotLayout

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, os.pardir, os.pardir)))

import themis.metaprograms.utils as metaprogram_utils
import utils as plot_utils

def style_plot(plot, stat_name):
    plot.setXLabel(stat_name)
    plot.setYLabel("Number of Samples")
    for line_style in ["solid", "dashed", "dotted"]:
        plot.addLineStyle(line_style)
    for color in ["red", "blue", "green", "violet", "black"]:
        plot.addLineColor(color)

def histogram_plot(experiment_log_dir, plot_spec_string, output_filename,
                   has_legend, x_limit, verbose):
    queries = [
        plot_utils.plot_spec_string_to_query(plot_spec_string, 0, "HIST")]

    plot_data = metaprogram_utils.process_queries(
        queries, experiment_log_dir, verbose)

    if "plot_points" not in plot_data:
        warnings.warn("No data to plot!")
        return

    histogram_data = plot_data["plot_points"][0]

    cumulative_histogram = {}

    layout = PlotLayout()
    layout.dpi = 250

    for stat_name in histogram_data:
        plot = Plot()
        plot.setTitle(stat_name)
        if has_legend:
            plot.hasLegend(labelSize=8)

        if x_limit is not None:
            plot.setXLimits(0, x_limit)

        style_plot(plot, stat_name)

        for key, points in sorted(histogram_data[stat_name].items()):
            for size, count in itertools.izip(points["bin"], points["count"]):
                if size not in cumulative_histogram:
                    cumulative_histogram[size] = 0
                cumulative_histogram[size] += count

            line = Line()
            line.stepFunction("pre")
            line.label = str(key)
            line.xValues = points["bin"]
            line.yValues = points["count"]
            plot.add(line)

        layout.addPlot(plot)

        cumulative_plot = Plot()

        if x_limit is not None:
            cumulative_plot.setXLimits(0, x_limit)

        cumulative_plot.setTitle("Cumulative Histogram for " + stat_name)
        style_plot(cumulative_plot, stat_name)
        line = Line()
        line.stepFunction("pre")
        line.xValues = sorted(cumulative_histogram.keys())
        line.yValues = [cumulative_histogram[key] for key in line.xValues]

        cumulative_plot.add(line)
        layout.addPlot(cumulative_plot)
    layout.save(output_filename)

def main():
    parser = argparse.ArgumentParser(
        description="plot a histogram statistic using data from an "
        "experiment's log file")
    parser.add_argument("experiment_log_dir", help="a directory containing log "
                        "data for an experiment")
    parser.add_argument("plot_spec_string", help="a dot-delimited plot "
                        "specification string of the form <hostname>."
                        "<phase name>.<stage name>.<worker ID>.<stat name>, "
                        "where any token can be replaced by a '*' to specify "
                        "\"any\"")
    parser.add_argument(
        "--output_filename", "-o",
        help="output filename (default: %(default)s)",
        default="histogram.png")
    parser.add_argument("--has_legend", "-l", help="display histogram plots "
                        "with legends", default=False, action="store_true")
    parser.add_argument("--x_limit", "-x", help="cut the histogram off at this "
                        "x-axis value", type=int)
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()

    histogram_plot(**vars(args))

if __name__ == "__main__":
    main()



