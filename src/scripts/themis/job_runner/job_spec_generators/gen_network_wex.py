#!/usr/bin/env python

import sys, argparse, json, utils

def gen_network_wex(input_directory, output_directory, **kwargs):
    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_wex_graph")

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory)

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "WEXLinkExtractorMapFunction",
        reduce_function = "WEXAdjacencyToPageRankReducer")

    if "params" not in config:
        config["params"] = {}

    config["params"]["MAP_INPUT_FORMAT_READER"] = "TextLineFormatReader"

    return config

def main():
    parser = argparse.ArgumentParser(
        description="generate a PageRank-compatible adjacency list from WEX "
        "input data")

    parser.add_argument("input_directory", help="the directory containing the "
                        "WEX dataset")
    parser.add_argument("--output_directory", help="the directory that will "
                        "contain the graph as an adjacency list (default: "
                        "a sibling directory of the input named "
                        "<input directory's name>_wex_graph)")
    parser.add_argument("-o", "--output_filename", help="name of the job "
                        "spec file to write (default: %(default)s)",
                        default="gen_network_wex.json")

    args = parser.parse_args()

    config = gen_network_wex(**vars(args))

    with open(args.output_filename, 'w') as fp:
        json.dump(config, fp, indent=2)


if __name__ == "__main__":
    sys.exit(main())
