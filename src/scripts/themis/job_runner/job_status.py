#!/usr/bin/env python

import sys, argparse, bottle, jinja2, os, time, socket, redis_utils

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir, os.pardir)))

import constants

coordinator_db = None

def datetimeformat(value, format='%m-%d-%Y %H:%M:%S'):
    return time.strftime(format, time.localtime(float(value)))

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(
        os.path.dirname(os.path.abspath(__file__))), trim_blocks=True)

jinja_env.filters["datetime"] = datetimeformat

@bottle.route('/')
def job_status():
    global coordinator_db, jinja_env

    cluster_info = {}

    dead_nodes = coordinator_db.dead_nodes

    for node in coordinator_db.known_nodes:
        cluster_info[node] = {}

        if node in dead_nodes:
            cluster_info[node]["status"] = "Dead"
        else:
            cluster_info[node]["status"] = "Alive"


        cluster_info[node]["dead_disks"] = coordinator_db.failed_local_disks(
            node)

        cluster_info[node]["live_disks"] = coordinator_db.local_disks(node)

    job_name_to_job_id = coordinator_db.job_names_and_ids

    incomplete_batches = coordinator_db.incomplete_batches

    job_info = {}
    nodes_remaining = {}

    for (job_name, job_id) in job_name_to_job_id.items():
        curr_job_info = coordinator_db.job_info(job_id)
        curr_job_info["job_name"] = job_name

        if "batch_id" in curr_job_info:
            nodes_remaining[job_id] = (
                coordinator_db.remaining_nodes_running_batch(
                    curr_job_info["batch_id"]))
        else:
            nodes_remaining[job_id] = "N/A"

        job_info[job_id] = curr_job_info

    template = jinja_env.get_template("job_status.jinja2")
    return template.render(job_info=job_info, log_directory=log_directory,
                           nodes_remaining = nodes_remaining,
                           cluster_info = cluster_info,
                           incomplete_batches = incomplete_batches)

@bottle.route('/batch_logs/<batch:int>/<path:path>')
def batch_logs(batch, path):
    global log_directory

    batch_log_path = os.path.abspath(
        '/'.join((log_directory, "run_logs", "batch_%d" % (batch))))

    dirname = os.path.abspath("%s/%s" % (batch_log_path, path))

    directories = []
    files = []

    if os.path.isdir(dirname):
        for basename in os.listdir(dirname):
            abspath = os.path.join(dirname, basename)

            link = "/batch_logs/%d/%s/%s" % (batch, path, basename)
            mod_time = os.path.getmtime(abspath)

            if os.path.isdir(abspath):
                size = '-'
                directories.append((link, basename, mod_time, size))
            else:
                size = str(os.path.getsize(abspath))
                files.append((link, basename, mod_time, size))

        files.sort()
        directories.sort()

        if dirname != batch_log_path:
            parent_path = os.path.dirname(dirname)
            parent_link = "/batch_logs/%d/%s" % (
                batch, os.path.relpath(parent_path, batch_log_path))

            directories.insert(0, (parent_link, "Parent Directory",
                                   os.path.getmtime(parent_path), '-'))
        else:
            directories.insert(0, ("/", "Back to Job Status", time.time(), '-'))

        template = jinja_env.get_template("batch_logs.jinja2")
        return template.render(files=files, directories=directories,
                               dirname=dirname)
    else:
        return bottle.static_file(path, root=batch_log_path,
                                  mimetype="text/plain")


@bottle.route('/batch_logs/<batch:int>')
def batch_logs_root(batch):
    return batch_logs(batch, "/")

@bottle.route('/error_reports/<job:int>')
def error_report(job):
    global coordinator_db, jinja_env

    template = jinja_env.get_template("error_report.jinja2")

    return template.render(
        job_id = job,
        fail_message = coordinator_db.job_info(job)["fail_message"])

def main():
    global coordinator_db, log_directory

    parser = argparse.ArgumentParser(
        description="provides a GUI for information on job history")
    utils.add_redis_params(parser)
    parser.add_argument(
        "--port", "-p", help="port on which the GUI accepts HTTP connections",
        type=int, default=4280)
    parser.add_argument("log_directory", help="base log directory for the "
                        "coordinator")
    args = parser.parse_args()

    coordinator_db = redis_utils.CoordinatorDB(
        args.redis_host, args.redis_port, args.redis_db)

    log_directory = args.log_directory

    try:
        bottle.run(host='0.0.0.0', port=args.port)
    except socket.error, e:
        print e
        # Return error 42 to indicate that we can't bind, so that scripts
        # calling this one can handle that case specially
        return constants.CANNOT_BIND

if __name__ == "__main__":
    sys.exit(main())
