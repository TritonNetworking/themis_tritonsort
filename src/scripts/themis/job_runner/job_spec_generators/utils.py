import os

from string import translate

def mapreduce_job(input_dir, job_name, username,
                  map_function = "PassThroughMapFunction",
                  reduce_function = "IdentityReduceFunction",
                  partition_function = "HashedBoundaryListPartitionFunction",
                  params = None):

    # Remove spaces from job name.
    job_name = translate(job_name, None, " ")

    input_dir, intermediate_dir, output_dir = generate_urls(
        input_dir, "%s/intermediates/%s" % (username, job_name),
        "%s/outputs/%s" % (username, job_name))

    config_dict = {
        "input_directory" : input_dir,
        "intermediate_directory" : intermediate_dir,
        "output_directory" : output_dir,
        "map_function" : map_function,
        "reduce_function" : reduce_function,
        "partition_function" : partition_function,
        "job_title" : job_name # 'job_name' reserved for internal use
        }

    if params is not None:
        config_dict["params"] = params

    return config_dict

def force_single_partition(config):
    if "params" not in config:
        config["params"] = {}

    config["params"]["SKIP_PHASE_ZERO"] = 1
    config["partition_function"] = "SinglePartitionMergingPartitionFunction"

def merge_jobs_into_batch(*jobs):
    batch = []

    for job in jobs:
        batch.append(job)

    return batch

def generate_url(path):
    if path[0] == '/':
        path = path[1:]

    if path[:9] == "local:///":
        # Don't URL-ize URLs
        return

    url = "local:///%s" % (path)

    return url

def generate_urls(input_path, intermediate_path, output_path):
    return (
        generate_url(input_path),
        generate_url(intermediate_path),
        generate_url(output_path))

def sibling_directory(directory, sibling_name):
    return os.path.join(
        os.path.dirname(directory),
        sibling_name % {"dirname" : os.path.basename(directory)})

def run_in_sequence(*batches):
    batch_list = []

    for batch in batches:
        if not isinstance(batch, list):
            batch = [batch]
        batch_list.append(batch)

    return batch_list
