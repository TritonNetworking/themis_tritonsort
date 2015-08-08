#!/usr/bin/env python

import sys, argparse, utils, json

def pagerank(input_directory, output_directory, iterations, hdfs, **kwargs):
    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_with_pageranks")

    config_list = []

    current_input = input_directory
    current_output = None

    for iteration in xrange(iterations):
        if iteration == iterations - 1:
            current_output = output_directory
        else:
            current_output = utils.sibling_directory(
                input_directory,
                "%(dirname)s_pagerank_iteration_" + str(iteration + 1))

        (input_url, output_url) = utils.generate_urls(
            current_input, current_output, hdfs)

        config = utils.mapreduce_job(
            input_dir = input_url,
            output_dir = output_url,
            map_function = "PageRankMapFunction",
            reduce_function = "PageRankReduceFunction"
            )

        config_list.append(config)

        current_input = current_output

    pagerank_config = utils.run_in_sequence(*config_list)

    return pagerank_config

def main():
    parser = argparse.ArgumentParser(description="run PageRank over a graph")
    parser.add_argument(
        "input_directory", help="the directory containing the graph over which "
        "PageRank will be run")
    parser.add_argument(
        "iterations", type=int, help="the number of iterations of the PageRank "
        "algorithm to run")
    parser.add_argument(
        "--output_directory", help="the directory that will contain the final "
        "graph annotated with ranks (default: a sibling of the input directory "
        "with the name '<input directory's name>_with_pageranks')")
    parser.add_argument(
        "--hdfs", help="host:port specifying the HDFS namenode where input and "
        "output data should be stored (default: store data on local disks "
        "rather than on HDFS)")
    parser.add_argument(
        "-o", "--output_filename", help="name of the job spec file to write "
        "(default: %(default)s)", default="pagerank.json")

    args = parser.parse_args()
    config = pagerank(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)

if __name__ == "__main__":
    sys.exit(main())
