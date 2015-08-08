import os, sys, subprocess, shlex, json, re

from LogLineDescription import load_descriptions_from_file

LOGGREP_PATH = os.path.abspath(os.path.join(
        os.path.dirname(__file__), os.pardir, os.pardir, os.pardir, os.pardir,
        "loggrep", "loggrep"))

def match_queries_to_descriptions(queries, descriptions):
    """
    When given a list of StatQuery objects and a list of LogLineDescription
    objects, attempts to match each query in the StatQuery list to a single
    description in the LogLineDescription list.
    """
    for query in queries:
        field_names = query.field_names

        matching_descriptions = filter(
            lambda x: x.matches_query(query),
            descriptions)

        if len(matching_descriptions) == 0:
            print >>sys.stderr, ("Couldn't match query %s to any description. "
                                 "Valid descriptions are:\n%s" %
                                 (query, '\n'.join(map(str, descriptions))))
            return False
        elif len(matching_descriptions) > 1:
            print >>sys.stderr, (
                "Multiple matching descriptions for query %s: %s" % (
                    query, '\n' + '\n'.join(map(str, matching_descriptions))))
            return False
        else:
            query.description = matching_descriptions[0]

    return True

def load_experiment_files(experiment_log_directory, skipped_phases=[]):
    """
    Returns a collection of (job name, phase name, hostname, file list)
    4-tuples. Each file in the file list is a triple consisting of the stats
    filename, the params filename, and the LogLineDescription object
    corresponding to the log line descriptions for that stats file.
    """

    experiment_log_directory = os.path.abspath(experiment_log_directory)

    assert os.path.exists(experiment_log_directory), (
        "Can't find directory %s" % (experiment_log_directory))

    batch_name = re.match("batch_\d+", os.path.basename(
            experiment_log_directory))

    assert batch_name is not None, ("Expected log directory name to be of "
                                    "the form `batch_#`")
    batch_name = batch_name.group(0)

    experiment_files = []

    for (dir_path, child_directories, filenames) in \
            os.walk(experiment_log_directory):

        # Only consider leaf directories
        if len(child_directories) > 0:
            continue

        # Treat the batch name as the job name. The directory should contain
        # directories for each phase (including multiple runs of phase zero,
        # which we'll treat separately)
        job_name = batch_name
        phase_name = os.path.basename(dir_path)

        # Check to see if this phase should be skipped.
        base_phase_name = phase_name
        job_suffix_position = phase_name.find("_job")
        if job_suffix_position != -1:
            base_phase_name = phase_name[:job_suffix_position]
        if base_phase_name in skipped_phases:
            # We should skip this phase.
            continue

        stat_log_files = []
        params_files = []
        descriptor_files = []

        for filename in filenames:
            for (suffix, file_list) in [
                ("_stats.log", stat_log_files),
                ("_params.log", params_files),
                ("_stat_descriptors.log", descriptor_files)]:

                suffix_index = filename.find(suffix)

                if suffix_index != -1:
                    file_list.append(os.path.join(dir_path, filename))

        if len(stat_log_files) != len(params_files) or \
            len(params_files) != len(descriptor_files):
            print "Some required files missing from %s (%d stat log files, " \
                "%d params files, %d descriptor files). Skipping %s" % (
                dir_path, len(stat_log_files), len(params_files),
                len(descriptor_files), base_phase_name)
            continue

        stat_log_files.sort()
        params_files.sort()
        descriptor_files.sort()

        descriptions = [load_descriptions_from_file(os.path.join(dir_path, f))
                        for f in descriptor_files]

        collated_files = zip(stat_log_files, params_files, descriptions)

        for file_set in collated_files:
            stat_filename = os.path.basename(file_set[0])
            hostname = stat_filename[:stat_filename.find("_stats.log")]

            experiment_files.append((job_name, phase_name, hostname, file_set))

    if len(experiment_files) == 0:
        raise ValueError("Didn't find any experiment log files in '%s'. Are you sure "
                 "that it is a top-level experiment directory?" %
                 (experiment_log_directory))
    return experiment_files

def run_queries_on_log_file(
    queries, log_file, job_name, hostname, output_data, verbose):
    """
    Run a list of queries on a given log file
    """

    if verbose:
        print "Running queries on '%s' ..." % (log_file)

    regexPatterns = [query.get_regex().pattern.replace('\t', '\\t')
                     for query in queries]

    loggrep_regex_args = ' '.join('"%s"' % (pattern)
                                  for pattern in regexPatterns)

    loggrep_command_str = "%(loggrep)s %(log_file)s %(args)s" % {
        "loggrep" : LOGGREP_PATH,
        "log_file" : log_file,
        "args" : loggrep_regex_args}

    loggrep_cmd = subprocess.Popen(shlex.split(loggrep_command_str),
                                   stdout=subprocess.PIPE,
                                   stderr=subprocess.PIPE)

    (stdout, stderr) = loggrep_cmd.communicate()

    if loggrep_cmd.returncode != 0:
        sys.exit("An error occurred while processing '%s': %s" % (
                loggrep_command_str, stderr))

    for line in stdout.split('\n'):
        line = line.strip()

        if len(line) == 0:
            continue

        line_chunks = line.split('\t')

        query_number = int(line_chunks[0])
        match_parts = line_chunks[1:]

        matching_query = queries[query_number]

        match_dict = matching_query.description.log_line_to_dict(match_parts)

        # Add information about the job and host to the match dictionary
        match_dict["hostname"] = hostname
        match_dict["job_name"] = job_name

        matching_query.match_processor_function(match_dict, output_data)

def process_queries_for_leaf_dir(queries, job_name, hostname,
                                 file_list, output_data, verbose):
    stats_file, params_file, stat_descriptions = file_list

    if not match_queries_to_descriptions(queries, stat_descriptions):
        sys.exit("Matching queries to descriptions failed")

    run_queries_on_log_file(queries, stats_file, job_name, hostname,
                            output_data, verbose)


def process_queries(
    queries, experiment_log_directory, verbose, skipped_phases=[]):
    """
    Run a set of queries across every log file in an experiment log directory.
    """

    log_file_lists = load_experiment_files(
        experiment_log_directory, skipped_phases)

    if not os.path.exists(LOGGREP_PATH):
        sys.exit("Can't find '%s'; be sure you've compiled loggrep and, "
                 "if you're doing an out-of-source build, that you've "
                 "symlinked loggrep into this location" % (LOGGREP_PATH))

    output_data = {}

    # Each 4-tuple produced by load_experiment_files represents a list of files
    # in a leaf directory of the experiment log directory; process each of
    # these in turn
    for (job_name, phase_name, hostname, file_list) in log_file_lists:
        process_queries_for_leaf_dir(
            queries, job_name, hostname, file_list, output_data, verbose)

    return output_data

def populate_nested_dictionary(dictionary, key_list, types=[]):
    """
    For each key k_i in key_list, make sure that
    dictionary[k_0][k_1][k_2]...[k_i] exists, creating objects of the
    appropriate type in each sub-dictionary as needed.

    If types is specified, then dictionary[key_list[i]] should point to an
    object of type types[i] for all i

    returns the element corresponding to dictionary[k_0][k_1]...[k_{n-1}] where
    n is the size of key_list
    """

    if len(key_list) > 0:
        if key_list[0] not in dictionary:
            if len(types) > 0:
                dictionary[key_list[0]] = types[0]()
            else:
                dictionary[key_list[0]] = {}

        return populate_nested_dictionary(dictionary[key_list[0]], key_list[1:],
                                          types[1:])
    else:
        return dictionary

def job_sequence(experiment_directory):
    return [os.path.basename(os.path.abspath(experiment_directory))]

def job_description(experiment_directory, job_name):
    # Assume that the description files were copied to the experiment dir.
    return experiment_directory
