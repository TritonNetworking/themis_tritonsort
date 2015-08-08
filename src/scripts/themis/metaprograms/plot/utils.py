import os, sys, re

sys.path.append(
    os.path.abspath(
        os.path.join(os.path.dirname(os.path.abspath(__file__)),
                     os.pardir, os.pardir, os.pardir)))

from themis.metaprograms.utils import StatQuery, StatContainer
import themis.metaprograms.utils as metaprogram_utils

def gather_timestamped_points_matcher(query_number, match, data, data_key):
    timestamp = float(match["timestamp"])
    phase = match["phase_name"]
    stat_name = match["collection_stat_name"]
    stat_val = match["value"]

    # Update minimum and maximum timestamps
    for key, comparison_function in [("min_timestamp", min),
                                     ("max_timestamp", max)]:
        if key not in data:
            data[key] = {}

        if phase not in data[key]:
            data[key][phase] = timestamp
        else:
            data[key][phase] = comparison_function(
                data[key][phase], timestamp)

    # Subdivide points by query, then further by stat name, and worker
    # identification so that we can make fine-grained plots if needed
    data_subdict = metaprogram_utils.populate_nested_dictionary(
        data, ["plot_points", query_number, stat_name, data_key])

    for key in ["x_values", "y_values"]:
        if key not in data_subdict:
            data_subdict[key] = []

    data_subdict["x_values"].append(timestamp)
    data_subdict["y_values"].append(stat_val)

def gather_histogram_points_matcher(query_number, match, data, data_key):
    phase = match["phase_name"]
    stat_name = match["stat_name"]
    bin_value = match["bin"]
    count = match["count"]

    data_subdict = metaprogram_utils.populate_nested_dictionary(
        data, ["plot_points", query_number, stat_name, data_key])

    for key in ["bin", "count"]:
        if key not in data_subdict:
            data_subdict[key] = []

    data_subdict["bin"].append(bin_value)
    data_subdict["count"].append(count)

def gen_logger_query(
    query_number, stat_type, hostname, phase, logger_name, stat):
    """
    This function generates general time-series log queries. In general,
    time-series logs can be logged by anything as long as the logger provides a
    logger name that distinguishes it from other loggers. In particular,
    queries of this form cover stats logged by memory allocators and stage
    trackers.
    """
    query_parts = [stat_type, ("phase_name", phase), ("epoch", None),
                   ("logger_name", logger_name)]

    if stat_type == "COLL":
        query_parts.append(("collection_stat_name", stat))
    elif stat_type == "HIST":
        query_parts.append(("stat_name", stat))

    query = StatQuery(*query_parts)

    if hostname is not None:
        hostname_regex = re.compile(hostname)
    else:
        hostname_regex = None

    def match_function(match, data):
        match_hostname = match["hostname"].split('.')[0]
        phase = match["phase_name"]
        logger_name = match["logger_name"]

        if (hostname is not None and hostname_regex is not None and
            hostname_regex.match(match_hostname) is None):
            return

        if stat_type == "COLL":
            stat_type_matcher_function = gather_timestamped_points_matcher
        elif stat_type == "HIST":
            stat_type_matcher_function = gather_histogram_points_matcher

        stat_type_matcher_function(
            query_number, match, data, (match_hostname, phase, logger_name))


    query.match_processor_function = match_function

    return query

def gen_worker_query(
    query_number, stat_type, hostname, phase, stage, worker_id, stat):
    """
    This function generates a worker stat query. Worker stat queries operate
    over statistics logged by individual workers. As such, they specify either
    a particular worker ID or all worker IDs. If the query applies to all
    worker IDs, worker_id is None.
    """
    if worker_id is not None:
        worker_id = int(worker_id)

    query_parts = [stat_type, ("phase_name", phase), ("epoch", None),
                   ("stage_name", stage), ("id", worker_id)]

    if stat_type == "COLL":
        query_parts.append(("collection_stat_name", stat))
    elif stat_type == "HIST":
        query_parts.append(("stat_name", stat))

    query = StatQuery(*query_parts)

    if hostname is not None:
        hostname_regex = re.compile(hostname)
    else:
        hostname_regex = None

    def match_function(match, data):
        match_hostname = match["hostname"].split('.')[0]
        phase = match["phase_name"]
        stage = match["stage_name"]
        worker_id = match["id"]

        if (hostname is not None and hostname_regex is not None and
            hostname_regex.match(match_hostname) is None):
            return

        if stat_type == "COLL":
            stat_type_matcher_function = gather_timestamped_points_matcher
        elif stat_type == "HIST":
            stat_type_matcher_function = gather_histogram_points_matcher

        stat_type_matcher_function(
            query_number, match, data,
            (match_hostname, phase, stage, worker_id))

    query.match_processor_function = match_function
    return query

def plot_spec_string_to_query(plot_spec_string, query_number, stat_type):
    plot_spec_chunks = plot_spec_string.split('.')

    for i, chunk in enumerate(plot_spec_chunks):
        if chunk == "*":
            # Convert '*' to None, since None denotes "any" in StatQueries.
            plot_spec_chunks[i] = None
        elif chunk.find("*") != -1:
            # Convert "*" anywhere in the chunk to greedy consumption (.*?)
            plot_spec_chunks[i] = chunk.replace("*", ".*?")

    if len(plot_spec_chunks) == 5:
        return gen_worker_query(query_number, stat_type, *plot_spec_chunks)
    elif len(plot_spec_chunks) == 4:
        return gen_logger_query(query_number, stat_type, *plot_spec_chunks)
    else:
        sys.exit("Malformed plot spec string '%s'" % (plot_spec_string))
