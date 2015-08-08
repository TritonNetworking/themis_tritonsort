#!/usr/bin/env python

import sys, argparse, os
import requests, plumbum

from plumbum.cmd import curl

def upload_file(input_file, output_file, hdfs_host):
    if output_file[0] == '/':
        output_file = output_file[1:]

    request_params = {
        "op" : "CREATE"
        }

    redirect_url = "http://%s/webhdfs/v1/%s" % (hdfs_host, output_file)

    redirect_request = requests.put(
        redirect_url,
        params=request_params, allow_redirects=False,
        config={"trust_env" : False})

    if redirect_request.status_code != 307:
        print >>sys.stderr, (
            "Expected status code 307 when creating %s, but got %d"
            % (input_file, redirect_request.status_code))
        sys.exit(1)

    redirect_location = redirect_request.headers['location']

    print "Uploading '%s' ..." % (input_file)

    upload_cmd = curl[
        '-s', '-o', '/dev/null', '-w', '"%{http_code}"', '--noproxy', '*', "-T",
        os.path.abspath(input_file), "-X", "PUT", redirect_location]

    try:
        response = int(upload_cmd().replace('"', ''))
    except plumbum.ProcessExecutionError, e:
        print "Command '%s' failed" % (upload_cmd)
        print e
        sys.exit(1)

    if response != 201:
        print "Command '%s' returned status code %d" % (upload_cmd, response)
        sys.exit(1)

def bulk_upload_directory(input_directory, output_directory, hdfs_host):
    for dirname, dirnames, filenames in os.walk(input_directory):
        relative_directory = os.path.relpath(dirname, input_directory)

        if relative_directory == ".":
            relative_directory = ""

        for filename in filenames:
            input_file = os.path.join(dirname, filename)
            output_file = os.path.join(
                "/", relative_directory, output_directory, filename)

            upload_file(input_file, output_file, hdfs_host)

def hdfs_bulk_upload(input_directories, output_directory, hdfs_host):
    # Create output directory
    if output_directory[0] == '/':
        output_directory = output_directory[1:]

    request_params = {
        "op" : "MKDIRS"
        }

    mkdir_request = requests.put(
        "http://%s/webhdfs/v1/%s" % (hdfs_host, output_directory),
        params=request_params, config={"trust_env": False})

    if mkdir_request.status_code != 200:
        print >>sys.stderr, ("Creating directory %s failed with code %d" % (
                output_directory, mkdir_request.status_code))

        sys.exit(1)

    for input_directory in input_directories:
        bulk_upload_directory(input_directory, output_directory, hdfs_host)

def main():
    parser = argparse.ArgumentParser(description="")
    parser.add_argument("hdfs_host", help="host:port of the HDFS namenode")
    parser.add_argument(
        "input_directories", nargs='+', help="directories whose files will be "
        "uploaded")
    parser.add_argument(
        "output_directory", help="directory that will contain the files")

    args = parser.parse_args()
    return hdfs_bulk_upload(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
