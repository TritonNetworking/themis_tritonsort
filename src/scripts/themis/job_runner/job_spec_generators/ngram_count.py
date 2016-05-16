#!/usr/bin/env python

import sys, argparse, utils, json

def ngram_count(input_directory, output_directory, ngram_count, **kwargs):
    if output_directory is None:
        output_directory = utils.sibling_directory(
            input_directory, "%(dirname)s_" + ("%dgram_counts" % (ngram_count)))

    (input_url, output_url) = utils.generate_urls(
        input_directory, output_directory)

    config = utils.mapreduce_job(
        input_dir = input_url,
        output_dir = output_url,
        map_function = "NGramMapFunction",
        reduce_function = "WordCountReduceFunction")

    if "params" not in config:
        config["params"] = {}

    config["params"]["NGRAM_COUNT"] = ngram_count

    return config

def main():
    parser = argparse.ArgumentParser(
        description="count occurrences of n-grams in a corpus of text")
    parser.add_argument(
        "input_directory", help="the directory containing the documents whose "
        "n-grams should be counted")
    parser.add_argument(
        "ngram_count", help="the size of n-grams that are produced; e.g. 3 "
        "for 3-grams, 5 for 5-grams (default: %(default)s)", type=int,
        default=5)
    parser.add_argument(
        "--output_directory", help="the directory that will contain the n-gram "
        "counts (default: a sibling of the input directory with the name "
        "<input directory's name>_Xgram_counts, where X is the value of the "
        "<ngram_count> argument)")
    parser.add_argument(
        "-o", "--output_filename", help="name of the job spec file to write "
        "(default: Xgram_count.json, where X is the value of the <ngram_count> "
        "argument)", default="Xgram_count.json")

    args = parser.parse_args()
    config = ngram_count(**vars(args))

    with open(str(args.ngram_count) + args.output_filename[1:], 'w') as fp:
        json.dump(config, fp, indent=2)

if __name__ == "__main__":
    sys.exit(main())
