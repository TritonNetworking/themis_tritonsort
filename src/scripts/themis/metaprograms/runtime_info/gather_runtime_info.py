#!/usr/bin/env python

import argparse, os, sys, json, pprint

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__),
                                             os.pardir,
                                             os.pardir,
                                             os.pardir)))

from themis.metaprograms.utils import StatContainer, StatQuery
import themis.metaprograms.utils as utils
from descriptions.descriptionutils import Description

def stat_container_append_matcher(data_key, stat_value_key):
    def matcher(stat, data):
        hostname = stat["hostname"]
        job = stat["job_name"]
        phase = stat["phase_name"]
        epoch = stat["epoch"]

        if "stage_name" in stat:
            stage_name = stat["stage_name"]

            colon_index = stage_name.find(':')

            if colon_index != -1:
                stage_name = stage_name[:colon_index]

            worker_id = stat["id"]
        else:
            stage_name = stat["logger_name"]
            worker_id = None

        val = stat[stat_value_key]

        # Add value to global job statistics
        global_data_key_list = [
            "stats", (job, phase, epoch), (stage_name,), data_key]

        global_stat_container = utils.populate_nested_dictionary(
            data, key_list=global_data_key_list,
            types=[dict, dict, dict, StatContainer])

        global_stat_container.append(val)

        # Add value to per-host statistics
        per_host_data_key_list = [
            "stats", (job, phase, epoch), (hostname, stage_name), data_key]

        per_host_stat_container = utils.populate_nested_dictionary(
            data, key_list=per_host_data_key_list,
            types=[dict, dict, dict, StatContainer])

        per_host_stat_container.append(val)

        # If this is a statistic pertaining to a worker, add value to per-worker
        # statistics
        if worker_id is not None:
            per_worker_data_key_list = [
                "stats", (job, phase, epoch), (hostname, stage_name, worker_id),
                data_key]

            per_worker_stat_container = utils.populate_nested_dictionary(
                data, key_list=per_worker_data_key_list,
                types=[dict, dict, dict, StatContainer])

            per_worker_stat_container.append(val)

    return matcher


def set_stage_value_matcher(stat, data):
    """
    Operating under the assumption that an entire stage's workers share one
    value for a statistic, this matcher will log the statistic for that stage,
    overwriting previous values if they exist
    """

    job = stat["job_name"]
    phase = stat["phase_name"]
    epoch = stat["epoch"]
    stage_name = stat["stage_name"]
    stat_name = stat["stat_name"]

    colon_index = stage_name.find(':')

    if colon_index != -1:
        stage_name = stage_name[:colon_index]

    if "str_value" in stat:
        stat_val = stat["str_value"]
    else:
        stat_val = stat["uint_value"]

    data_subdict = utils.populate_nested_dictionary(
        data, ["stage_info", (job, phase, epoch), stage_name])

    data_subdict[stat_name] = stat_val


def postprocess_workers(workers_info, stage_info, stat_key_length):
    """
    Calculates derived statistics from those printed in the logs.

    I/O multiplier =  bytes out /  bytes in

    total processing time = runtime + teardown time + idle time

    For pull-based workers (that idle while running):
     * total processing time -= (idle time + pipeline saturation time)

    useful processing time = total processing time - idle time -
    memory wait time - enqueue wait time

    % total time waiting for buffers = time waiting for memory / total
      processing time

    % total time blocked on enqueue = enqueue block time /
      total processing time

    % time waiting for work = idle time / total processing time

    % useful time = useful processing time / total processing time

    observed processing rate = total bytes in / max runtime
    ideal processing rate = observed processing rate / % useful time

    % ios would have blocked = # ios would have blocked / total # ios

    """

    post_dict = dict(workers_info)

    # Convert all integer values in post_dict to floats so that divisions work
    # without all the extra casts

    if "total_mem_wait_time" not in post_dict:
        post_dict["total_mem_wait_time"] = StatContainer()

    # Compute absolute start and stop times
    num_values = 0
    worker_runtime = None
    if "worker_start_time" in post_dict:
        num_values = post_dict["worker_start_time"].count()
        post_dict["worker_start_time"] = post_dict["worker_start_time"].min()
    if "worker_stop_time" in post_dict:
        assert post_dict["worker_stop_time"].count() == num_values, \
            "Should have %d values but have %d. Stage info %s" % (
            num_values, post_dict["worker_stop_time"].count(), stage_info)
        post_dict["worker_stop_time"] = post_dict["worker_stop_time"].max()
        worker_runtime = \
            post_dict["worker_stop_time"] - post_dict["worker_start_time"]
        post_dict["worker_runtime"] = worker_runtime

    if worker_runtime is not None:
        post_dict["worker_runtime"] = worker_runtime
    else:
        post_dict["worker_runtime"] = 0

    # Replace some stat containers with their totals
    for key in ["total_runtime", "total_idle_time",
                "total_teardown_time", "total_bytes_in", "total_bytes_out",
                "total_mem_wait_time", "total_enqueue_block_time",
                "would_have_blocked", "total_ios"]:
        if key in post_dict:
            post_dict[key] = post_dict[key].sum()

    if "stage_runtime" in post_dict:
        post_dict["mean_stage_runtime"] = post_dict["stage_runtime"].mean()
        post_dict["max_stage_runtime"] = post_dict["stage_runtime"].max()
        del post_dict["stage_runtime"]

    for key, value in post_dict.items():
        if type(value) == int:
            post_dict[key] = float(value)

    if post_dict["total_bytes_in"] > 0:
        post_dict["io_multiplier"] = (
            post_dict["total_bytes_out"] / post_dict["total_bytes_in"])
    else:
        post_dict["io_multiplier"] = None

    if "num_workers" in post_dict:
        post_dict["num_workers"] = post_dict["num_workers"].max()

    post_dict["pipeline_saturation_time"] = post_dict[
        "pipeline_saturation_time"].mean()

    post_dict["total_processing_time"] = (
        post_dict["total_runtime"] + post_dict["total_teardown_time"] +
        post_dict["total_idle_time"])

    if stage_info["worker_type"] == "pull":
        # Pull based workers log idle and pipeline saturation times from
        # with run(), so we have to subtract these off to get an accurate
        # processing time
        post_dict["total_processing_time"] -= (
            post_dict["total_idle_time"] +
            post_dict["pipeline_saturation_time"])

    post_dict["useful_processing_time"] = (
        post_dict["total_processing_time"] -
        post_dict["total_idle_time"] - post_dict["total_mem_wait_time"] -
        post_dict["total_enqueue_block_time"])

    if post_dict["total_processing_time"] > 0:
        post_dict["percent_time_waiting_for_work"] = (
            post_dict["total_idle_time"] /
            post_dict["total_processing_time"])

        post_dict["percent_total_time_waiting_for_buffers"] = (
            post_dict["total_mem_wait_time"] /
            post_dict["total_processing_time"])

        post_dict["percent_total_time_blocked_on_enqueue"] = (
            post_dict["total_enqueue_block_time"] /
            post_dict["total_processing_time"])

        post_dict["percent_useful_processing"] = (
            post_dict["useful_processing_time"] /
            post_dict["total_processing_time"])

        if worker_runtime is not None:
            post_dict["observed_processing_rate"] = (
                post_dict["total_bytes_in"] / worker_runtime)
        else:
            post_dict["observed_processing_rate"] = 0

        # Compute the throughput we'd be able to achieve if we didn't have to
        # wait or block.
        post_dict["ideal_processing_rate"] = (
            post_dict["observed_processing_rate"] /
            post_dict["percent_useful_processing"])

        # Compute per worker and per host rates if necessary.
        # num_values is the number of data points we got.
        # There are 4 cases:
        #   stat_key_length = 3 && num_values = 1
        #     We're handling a specific worker ID
        #   stat_key_length = 2 && num_values = num_workers
        #     We're handling a stage on an individual host
        #   stat key length = 1 && num_workers | num_values
        #     We're handling a stage in the cluster.
        #     In this case num_hosts = num_values / num_workers
        #   else
        #     We're missing data points, perhaps as a result of asymmetric
        #     pipeline configurations on nodes (eg. phase 0 coordinator).

        num_workers = None
        if "num_workers" in post_dict:
            num_workers = post_dict["num_workers"]

        # Case 1
        if stat_key_length == 3 and num_values == 1:
            # We're looking at a specific worker, so the total throughput is the
            # per-worker throughput. The per_worker and per_node data values
            # won't be displayed, so just set them to 0.
            post_dict["observed_processing_rate_per_worker"] = 0
            post_dict["observed_processing_rate_per_node"] = 0
            post_dict["ideal_processing_rate_per_worker"] = 0
            post_dict["ideal_processing_rate_per_node"] = 0
        # Case 2
        elif stat_key_length == 2 and num_values == num_workers:
            # We're looking at a specific host, so the total throughput is the
            # per-host throughput. Set the per-host data value to 0 since it
            # won't be displayed, but set the per-worker data value as
            #     total / num_workers
            post_dict["observed_processing_rate_per_worker"] = (
                post_dict["observed_processing_rate"] / num_workers)
            post_dict["observed_processing_rate_per_node"] = 0
            post_dict["ideal_processing_rate_per_worker"] = (
                post_dict["ideal_processing_rate"] / num_workers)
            post_dict["ideal_processing_rate_per_node"] = 0
        # Case 3
        elif stat_key_length == 1 and num_values % num_workers == 0:
            # We're looking at the entire cluster. Compute the number of hosts
            #     num_values / num_workers
            # And then compute the per-node throughput as
            #     total / num_nodes
            # And the per-worker throughput as
            #     total / num_values
            # Since num_workers represents the number of workers per node, and
            # num_values is going to be the total number of workers in the
            # cluster.
            num_hosts = num_values / num_workers

            # If we didn't actually collect values for this phase (for example
            # if the phase was short-circuited with no work), then set the
            # denominators to 1 so we don't divide by 0.
            if num_values == 0:
                num_values = 1
            if num_hosts == 0:
                num_hosts = 1
            post_dict["observed_processing_rate_per_worker"] = (
                post_dict["observed_processing_rate"] / num_values)
            post_dict["observed_processing_rate_per_node"] = (
                post_dict["observed_processing_rate"] / num_hosts)
            post_dict["ideal_processing_rate_per_worker"] = (
                post_dict["ideal_processing_rate"] / num_values)
            post_dict["ideal_processing_rate_per_node"] = (
                post_dict["ideal_processing_rate"] / num_hosts)
        # Case 4
        else:
            # We're missing data points perhaps as result of asymmetric worker
            # configuration (eg phase 0) Since we can't do any better, just set
            # the per-worker and per-node values to 0.
            post_dict["observed_processing_rate_per_worker"] = 0
            post_dict["observed_processing_rate_per_node"] = 0
            post_dict["ideal_processing_rate_per_worker"] = 0
            post_dict["ideal_processing_rate_per_node"] = 0
    else:
        post_dict["percent_total_time_waiting_for_buffers"] = 0
        post_dict["percent_total_time_blocked_on_enqueue"] = 0
        post_dict["percent_time_waiting_for_work"] = 0
        post_dict["observed_processing_rate"] = 0
        post_dict["ideal_processing_rate"] = 0
        post_dict["observed_processing_rate_per_worker"] = 0
        post_dict["observed_processing_rate_per_node"] = 0
        post_dict["ideal_processing_rate_per_worker"] = 0
        post_dict["ideal_processing_rate_per_node"] = 0

    if "would_have_blocked" in post_dict and "total_ios" in post_dict and \
            post_dict["total_ios"] > 0.0:
        post_dict["percent_would_have_blocked"] = (
            post_dict["would_have_blocked"] / post_dict["total_ios"])

    # Make sure every stat is at least zero
    for key in post_dict:
        if type(post_dict[key]) in [int, float]:
            if post_dict[key] == -0.0:
                # I hate you floating point
                post_dict[key] = 0
            else:
                post_dict[key] = max(post_dict[key], 0.0)

    return post_dict

def description_order(description, phase):
    stage_ordering = description.getStageOrdering(phase)

    def comp(stage_key):
        hostname = None
        stage = None
        worker_id = None

        if len(stage_key) == 1:
            stage = stage_key[0]
        elif len(stage_key) == 2:
            hostname, stage = stage_key
        elif len(stage_key) == 3:
            hostname, stage, worker_id = stage_key
        else:
            sys.exit("Unexpected stage stat key %s" % (stage_stat_key))

        try:
            stage_index = stage_ordering.index(stage)
        except ValueError, e:
            stage_index = None

        return (len(stage_key), hostname, stage_index, worker_id)
    return comp

def postprocess_epoch(epoch_info, stage_info, phase, description):
    post_info_dict = {}
    stage_key_order = epoch_info.keys()

    if description is not None:
        stage_key_order.sort(key=description_order(description, phase))

    worker_stats_list = []

    for stage_stat_key in stage_key_order:
        if len(stage_stat_key) == 1:
            stage_name = stage_stat_key[0]
        else:
            stage_name = stage_stat_key[1]

        workers_info = postprocess_workers(
            epoch_info[stage_stat_key], stage_info[stage_name],
            len(stage_stat_key))

        stats_info_dict = {}

        if len(stage_stat_key) == 1:
            stats_info_dict["stage"] = stage_stat_key[0]
        elif len(stage_stat_key) == 2:
            stats_info_dict["hostname"] = stage_stat_key[0]
            stats_info_dict["stage"] = stage_stat_key[1]
        elif len(stage_stat_key) == 3:
            stats_info_dict["hostname"] = stage_stat_key[0]
            stats_info_dict["stage"] = stage_stat_key[1]
            stats_info_dict["worker_id"] = stage_stat_key[2]
        else:
            sys.exit("Unexpected stage stat key %s" % (stage_stat_key))

        workers_info["stats_info"] = stats_info_dict

        worker_stats_list.append(workers_info)

    post_info_dict["stages"] = worker_stats_list

    return post_info_dict

def epoch_comparator(job_sequence, phase_sequence):
    def comp(x):
        job, phase, epoch = x
        try:
            job_index = job_sequence.index(job)
        except ValueError, e:
            job_index = None

        try:
            phase_index = phase_sequence[job].index(phase)
        except ValueError, e:
            phase_index = None

        return (job_index, phase_index, None)
    return comp

def postprocess(runtime_info, experiment_directory):
    postprocessed_data = []

    stats_info = runtime_info["stats"]
    stage_info = runtime_info["stage_info"]

    # Emit epochs in the order in which they were executed
    job_sequence = utils.job_sequence(experiment_directory)

    descriptions = {}

    for job in job_sequence:
        description_dir = utils.job_description(experiment_directory, job)
        if os.path.exists(description_dir):
            descriptions[job] = Description(description_dir)

    phase_sequence = {}

    for job in job_sequence:
        if job in descriptions:
            phase_sequence[job] = descriptions[job].getPhaseList()
        else:
            phase_sequence[job] = []

    comp_function = epoch_comparator(job_sequence, phase_sequence)

    sorted_epochs = sorted(stats_info.keys(), key=comp_function)

    for (job, phase, epoch) in sorted_epochs:
        epoch_info = stats_info[(job, phase, epoch)]

        if job in descriptions:
            description = descriptions[job]
        else:
            description = None

        postprocessed_epoch_info = postprocess_epoch(
            epoch_info, stage_info[(job, phase, epoch)], phase, description)

        postprocessed_epoch_info["job"] = job
        postprocessed_epoch_info["phase"] = phase
        postprocessed_epoch_info["epoch"] = epoch

        postprocessed_data.append(postprocessed_epoch_info)

    return postprocessed_data

def gather_runtime_info(experiment_directory, verbose, skipped_phases=[]):
    total_runtime_query = StatQuery(
        "SUMM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "runtime"),
        ("summary_stat_name", "sum"))
    total_runtime_query.match_processor_function = \
        stat_container_append_matcher("total_runtime", "value")

    total_idle_time_query = StatQuery(
        "SUMM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "wait"),
        ("summary_stat_name", "sum"))
    total_idle_time_query.match_processor_function = \
        stat_container_append_matcher("total_idle_time", "value")

    pipeline_saturation_time_query = StatQuery(
        "SUMM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "pipeline_saturation_wait"),
        ("summary_stat_name", "sum"))
    pipeline_saturation_time_query.match_processor_function = (
        stat_container_append_matcher("pipeline_saturation_time", "value"))

    num_workers_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("logger_name", None),
        ("stat_name", "num_workers"),
        ("uint_value", None))
    num_workers_query.match_processor_function = stat_container_append_matcher(
        "num_workers", "uint_value")

    teardown_time_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "teardown"),
        ("start_time", None))
    teardown_time_query.match_processor_function = \
        stat_container_append_matcher("total_teardown_time", "elapsed_time")

    stage_runtime_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("logger_name", None),
        ("stat_name", "stage_runtime"),
        ("start_time", None))
    stage_runtime_query.match_processor_function = \
        stat_container_append_matcher("stage_runtime", "elapsed_time")

    input_size_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "bytes_consumed"),
        ("uint_value", None))
    input_size_query.match_processor_function = stat_container_append_matcher(
        "total_bytes_in", "uint_value")

    output_size_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "bytes_produced"),
        ("uint_value", None))
    output_size_query.match_processor_function = stat_container_append_matcher(
        "total_bytes_out", "uint_value")

    allocation_time_query = StatQuery(
        "SUMM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "allocation_wait_time"),
        ("summary_stat_name", "sum"))
    allocation_time_query.match_processor_function = \
        stat_container_append_matcher("total_mem_wait_time", "value")

    enqueue_block_time_query = StatQuery(
        "SUMM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "queue_saturation_block_time"),
        ("summary_stat_name", "sum"))
    enqueue_block_time_query.match_processor_function = \
        stat_container_append_matcher(
        "total_enqueue_block_time", "value")

    worker_type_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "worker_type"),
        ("str_value", None))
    worker_type_query.match_processor_function = set_stage_value_matcher

    would_have_blocked_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "would_have_blocked"),
        ("uint_value", None))
    would_have_blocked_query.match_processor_function = \
        stat_container_append_matcher("would_have_blocked", "uint_value")

    total_ios_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "total_ios"),
        ("uint_value", None))
    total_ios_query.match_processor_function = stat_container_append_matcher(
        "total_ios", "uint_value")

    worker_start_time_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "worker_start_time"),
        ("uint_value", None))
    worker_start_time_query.match_processor_function = \
        stat_container_append_matcher("worker_start_time", "uint_value")

    worker_stop_time_query = StatQuery(
        "DATM",
        ("phase_name", None),
        ("epoch", None),
        ("stage_name", None),
        ("id", None),
        ("stat_name", "worker_stop_time"),
        ("uint_value", None))
    worker_stop_time_query.match_processor_function = \
        stat_container_append_matcher("worker_stop_time", "uint_value")

    queries = [total_runtime_query, total_idle_time_query,
               pipeline_saturation_time_query, num_workers_query,
               teardown_time_query, stage_runtime_query, input_size_query,
               output_size_query, allocation_time_query,
               enqueue_block_time_query, worker_type_query,
               would_have_blocked_query, total_ios_query,
               worker_start_time_query, worker_stop_time_query]

    runtime_info = utils.process_queries(
        queries, experiment_directory, verbose, skipped_phases)

    runtime_info = postprocess(runtime_info, experiment_directory)

    return runtime_info

def main():
    parser = argparse.ArgumentParser(description="extract runtime information "
                                     "from logs for an experiment, including "
                                     "runtime with and without buffer waits")
    parser.add_argument("experiment_directory", help="path to the "
                        "experiment logs' root directory")
    parser.add_argument("--output_filename", "-o", help="path to the file "
                        "where runtime information will be written",
                        default="runtime_info.json")
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()

    runtime_info = gather_runtime_info(args.experiment_directory, args.verbose)

    with open(args.output_filename, 'w+') as fp:
        json.dump(runtime_info, fp, indent=4)

if __name__ == "__main__":
    main()
