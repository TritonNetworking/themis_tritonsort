#!/usr/bin/env python

import sys, argparse, json

def runner_job_converter(experiment_config_file, job_name, output):
    if output == None:
        output = "%s.json" % (job_name)

    with open(experiment_config_file, 'r') as fp:
        experiment_config = json.load(fp)

    job_config = experiment_config["jobs"][job_name]

    converted_config = {
        "map_function" : job_config["map"],
        "reduce_function" : job_config["reduce"],
        "input_directory" : job_config["input_directory"],
        "output_directory" : job_config["output_directory"]
        }

    with open(output, 'w') as fp:
        json.dump(converted_config, fp)

def main():
    parser = argparse.ArgumentParser(
        description="converts an experiment config file into a format "
        "suitable for use with the coordinator")
    parser.add_argument("experiment_config_file", help="path to the "
                        "experiment config file")
    parser.add_argument("job_name", help="the job name to convert")
    parser.add_argument("--output", "-o", help="the path to the converted job "
                        "specification file (default: <job_name>.json)")

    args = parser.parse_args()
    return runner_job_converter(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
