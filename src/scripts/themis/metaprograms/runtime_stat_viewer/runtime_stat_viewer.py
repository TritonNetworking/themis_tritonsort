#!/usr/bin/env python

import os, sys, argparse, json, bottle, jinja2

SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))

sys.path.append(os.path.join(SCRIPT_DIR, os.pardir))

from runtime_info.gather_runtime_info import gather_runtime_info

BASE_PATH = os.path.abspath(
    os.path.join(SCRIPT_DIR, os.pardir, os.pardir, os.pardir))

sys.path.append(BASE_PATH)

from descriptions.descriptionutils import Description
import common.unitconversion as uc

jinja_env = jinja2.Environment(
    loader = jinja2.FileSystemLoader(os.path.join(SCRIPT_DIR, 'templates')),
    trim_blocks = True)

runtime_infos = {}
log_directory = None
verbose = False

def get_runtime_info(dirname):
    if dirname in runtime_infos:
        runtime_info = runtime_infos[dirname]
    else:
        # We need to compute this runtime info
        try:
            runtime_info = gather_runtime_info(
                os.path.join(log_directory, dirname), verbose)
        except Exception as e:
            print e
            runtime_info = None

        runtime_infos[dirname] = runtime_info

    return runtime_info

def get_directory_contents(directory, prefix=""):
  try:
    directory_contents = os.listdir(directory)
    directory_contents = [x for x in directory_contents
                          if os.path.isdir(os.path.join(directory, x))]
    directory_contents.sort(
        key=lambda x: os.path.getmtime(os.path.join(directory, x)),
        reverse=True)
    directory_contents = [os.path.join(prefix, x) for x in directory_contents]
    return directory_contents
  except OSError:
    return None


@bottle.route("/")
def directory_listing():
  directory_contents = get_directory_contents(log_directory)
  template = jinja_env.get_template("directory_listing.jinja2")
  return template.render(directory_contents=directory_contents)


@bottle.route("/<dirname:path>/")
def global_stats(dirname):
  runtime_info = get_runtime_info(dirname)
  if runtime_info == None:
    directory_contents = get_directory_contents(
        os.path.join(log_directory, dirname),
        prefix=dirname)
    if directory_contents:
      template = jinja_env.get_template("directory_listing.jinja2")
      return template.render(directory_contents=directory_contents)
    else:
      template = jinja_env.get_template("no_runtime_info.jinja2")
      return template.render(dirname=dirname)

  template = jinja_env.get_template("overall_stats.jinja2")
  return template.render(dirname=dirname,
                         runtime_info=runtime_info,
                         log_path=os.path.join(log_directory, dirname))

@bottle.route("/<dirname:path>/by_hostname")
def stats_by_hostname(dirname):
    runtime_info = get_runtime_info(dirname)
    if runtime_info == None:
        template = jinja_env.get_template("no_runtime_info.jinja2")
        return template.render(dirname = dirname)

    job = bottle.request.query.job
    phase = bottle.request.query.phase
    epoch = bottle.request.query.epoch
    stage = bottle.request.query.stage

    template = jinja_env.get_template("host_stats.jinja2")
    return template.render(
        dirname = dirname, job=job, phase=phase, epoch=epoch, stage=stage,
        runtime_info = runtime_info,
        log_path=os.path.join(log_directory, dirname))

@bottle.route("/<dirname:path>/by_worker")
def stats_by_worker(dirname):
    runtime_info = get_runtime_info(dirname)
    if runtime_info == None:
        template = jinja_env.get_template("no_runtime_info.jinja2")
        return template.render(dirname = dirname)

    job = bottle.request.query.job
    phase = bottle.request.query.phase
    epoch = bottle.request.query.epoch
    hostname = bottle.request.query.hostname
    stage = bottle.request.query.stage

    template = jinja_env.get_template("worker_stats.jinja2")
    return template.render(
        dirname = dirname, job=job, phase=phase, epoch=epoch, stage=stage,
        hostname=hostname, runtime_info=runtime_info,
        log_path=os.path.join(log_directory, dirname))

@bottle.route('/static/<path:path>')
def static(path):
    return bottle.static_file(path, root=os.path.join(SCRIPT_DIR, "static"))

def main():
    global runtime_info, log_directory, verbose

    parser = argparse.ArgumentParser(
        description="web viewer for runtime statistics")
    parser.add_argument(
        "log_directory", help="directory containing logs for all experiments")
    parser.add_argument(
        "--port", "-p", help="port on which the web viewer runs", default=9090,
        type=int)
    parser.add_argument(
        "-v", "--verbose", default=False, action="store_true",
        help="Print detailed log processing information")

    args = parser.parse_args()
    log_directory = args.log_directory
    verbose = args.verbose

    if (not os.path.exists(log_directory) or
        not os.path.isdir(log_directory)):

        parser.error("'%s' doesn't exist or is not a directory" %
                     (log_directory))

    bottle.debug(True)
    return bottle.run(host='0.0.0.0', port=args.port)

if __name__ == "__main__":
    sys.exit(main())


