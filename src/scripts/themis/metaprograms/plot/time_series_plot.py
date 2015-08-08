#!/usr/bin/env python

import os, sys, argparse, warnings, pprint
from boomslang import Line, Plot, PlotLayout, Utils, Line

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, os.pardir, os.pardir)))

import themis.metaprograms.utils as metaprogram_utils
import utils as plot_utils

def create_plot_objects_for_group(phase_name, group_name, min_timestamp,
                                  max_timestamp, make_legend):
    """
    Create and format two Boomslang plot objects: one for the time series plot,
    and the other for a CDF of the y-axis values in the time series. min and
    max timestamps are assumed to be adjusted with timestamp_adjuster (see
    below) before being passed as arguments
    """

    time_series_plot = Plot()
    time_series_plot.setXLimits(
        0, (max_timestamp - min_timestamp) / 1000000.0)
    time_series_plot.setXLabel("Time (s)")
    time_series_plot.setYLabel(group_name)
    cdf_plot = Plot()
    cdf_plot.setXLabel(group_name)
    cdf_plot.setYLabel("CDF")

    for plot in [time_series_plot, cdf_plot]:
        if make_legend:
            plot.hasLegend(labelSize = 8, columns=2)

        plot.setTitle("Phase: %(phase_name)s, %(group_name)s" %
                      {"phase_name" : phase_name, "group_name" : group_name})

        for color in ["red", "green", "blue", "teal", "orange", "purple",
                      "brown", "black"]:
            plot.addLineColor(color)

        for style in ["solid", "dashed", "dotted"]:
            plot.addLineStyle(style)

    return (time_series_plot, cdf_plot)



def make_line_objects_for_stat_name(
    query_number, stat_name, stat_name_data, min_timestamps):

    lines = []

    for points_info in sorted(stat_name_data):
        lines_dict = {}
        lines_dict["query_number"] = query_number
        lines_dict["stat_name"] = stat_name

        hostname = points_info[0].split('.')[0][-3:]

        if len(points_info) == 4:
            (phase, stage, worker_id) = points_info[1:]
            lines_dict["stage"] = stage
            lines_dict["worker_id"] = worker_id
            label = "h:%s s:%s i:%d n:%s" % (
                hostname, stage, worker_id, stat_name)

        elif len(points_info) == 3:
            (phase, logger_name) = points_info[1:]
            lines_dict["logger_name"] = logger_name
            label = "h:%s l:%s n:%s" % (
                hostname, logger_name, stat_name)

        lines_dict["hostname"] = hostname
        lines_dict["phase"] = phase

        min_timestamp = min_timestamps[phase]

        def timestamp_adjuster(x):
            """
            Adjust an absolute timestamp in microseconds to time in seconds
            relative to the min timestamp
            """
            return float(x - min_timestamp) / 1000000.0

        time_series_line = Line()
        time_series_line.label = label
        time_series_line.stepFunction("pre")

        time_series_line.xValues = map(
            timestamp_adjuster, stat_name_data[points_info]["x_values"])
        time_series_line.yValues = stat_name_data[points_info]["y_values"]

        lines_dict["time_series_line"] = time_series_line
        lines_dict["cdf_line"] = Utils.getCDF(
            stat_name_data[points_info]["y_values"])
        lines_dict["cdf_line"].label = label

        lines.append(lines_dict)
    return lines

def make_plots_for_query(query_number, query_data, min_timestamps,
                         max_timestamps, make_legend, split_by_host,
                         group_by_query):
    """
    Generate a list of plots for all stats retrieved by a given query
    """
    plots = {}

    lines = []

    # Generate Line objects for every statistic of interest
    for stat_name in sorted(query_data):
        lines.extend(make_line_objects_for_stat_name(
                query_number, stat_name, query_data[stat_name], min_timestamps))

    grouped_lines = {}

    if group_by_query:
        if split_by_host:
            group_function = (
                lambda x: (x["query_number"], x["phase"], x["hostname"]))
            phase_index_in_group = 1
        else:
            group_function = lambda x: (x["query_number"], x["phase"])
            phase_index_in_group = 1
    else:
        if split_by_host:
            group_function = lambda x: (x["phase"], x["hostname"])
            phase_index_in_group = 0
        else:
            group_function = lambda x: (x["phase"], x["stat_name"])
            phase_index_in_group = 0

    for line in lines:
        group_key = group_function(line)

        if group_key not in grouped_lines:
            grouped_lines[group_key] = []
        grouped_lines[group_key].append(line)

    plots = {}

    min_y_value = None
    max_y_value = None

    for group in grouped_lines:
        phase = group[phase_index_in_group]

        time_series_plot, cdf_plot = create_plot_objects_for_group(
            phase, str(group), min_timestamps[phase], max_timestamps[phase],
            make_legend)

        for line_set in grouped_lines[group]:
            time_series_plot.add(line_set["time_series_line"])

            y_vals = [i for i in (min(line_set["time_series_line"].yValues),
                                  max(line_set["time_series_line"].yValues),
                                  min_y_value, max_y_value)
                      if i is not None]

            min_y_value = min(y_vals)
            max_y_value = max(y_vals)

            cdf_plot.add(line_set["cdf_line"])

        plots[group] = [time_series_plot, cdf_plot]

    for group in plots:
        plots[group][0].setYLimits(min_y_value, max_y_value)
    return plots

def make_plots(plot_data, make_legend, split_by_host, group_by_query):
    """
    Generate a list consisting of tuples of plot objects. These plot object
    tuples should be plotted in order, one tuple per line
    """

    if "min_timestamp" not in plot_data or "max_timestamp" not in plot_data:
        return {}

    min_timestamps = plot_data["min_timestamp"]
    max_timestamps = plot_data["max_timestamp"]

    plots = {}

    for query_number in sorted(plot_data["plot_points"].keys()):
        query_plots = make_plots_for_query(
            query_number, plot_data["plot_points"][query_number],
            min_timestamps, max_timestamps, make_legend, split_by_host,
            group_by_query)

        for key, value in query_plots.items():
            if key not in plots:
                plots[key] = []

            plots[key].extend(value)

    return plots

def time_series_plot(experiment_log_dir, plot_spec_strings,
                     make_legend, split_by_host, group_by_query, verbose):
    queries = []

    for i, plot_spec_string in enumerate(plot_spec_strings):
        queries.append(
            plot_utils.plot_spec_string_to_query(
                plot_spec_string, i, "COLL"))

    plot_data = metaprogram_utils.process_queries(
        queries, experiment_log_dir, verbose)
    plots = make_plots(plot_data, make_legend, split_by_host, group_by_query)

    if len(plots) == 0:
        warnings.warn("No data to plot!")
        return

    return plots

def save_time_series_plots(plots, output_directory):
    if os.path.exists(output_directory):
        for filename in os.listdir(output_directory):
            if filename[-3:] == "png":
                os.unlink(os.path.join(output_directory, filename))
        os.rmdir(output_directory)

    os.makedirs(output_directory)

    for group in sorted(plots.keys()):
        layout = PlotLayout()
        layout.dpi = 250

        output_filename = os.path.join(
            output_directory, "_".join(map(str, group)) + ".png")

        for subplot in plots[group]:
            layout.addPlot(subplot, grouping=group)

        layout.save(output_filename)

def main():
    parser = argparse.ArgumentParser(
        description="Plot information about time-series data from the logs")
    parser.add_argument("experiment_log_dir", help="a directory containing log "
                        "data for an experiment")
    parser.add_argument("plot_spec_string", help="a dot-delimited plot "
                        "specification string of the form <hostname>."
                        "<phase name>.<stage name>.<worker ID>.<stat name>, "
                        "where any token can be replaced by a '*' to specify "
                        "\"any\"", nargs="+")
    parser.add_argument("--no_legend", help="suppress legend in plot outputs",
                        default=False, action="store_true")
    parser.add_argument("--split_by_host", help="make a separate plot for each "
                        "hostname", default=False, action="store_true")
    parser.add_argument("--group_by_query", help="group lines by the query "
                        "that generated them", default=False,
                        action="store_true")
    parser.add_argument(
        "--output_directory", "-o", help="output directory name "
        "(default: %(default)s)", default="plots")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()

    plots = time_series_plot(experiment_log_dir = args.experiment_log_dir,
                             plot_spec_strings = args.plot_spec_string,
                             make_legend = not args.no_legend,
                             split_by_host = args.split_by_host,
                             group_by_query = args.group_by_query,
                             verbose = args.verbose)

    save_time_series_plots(plots, args.output_directory)

if __name__ == "__main__":
    main()
