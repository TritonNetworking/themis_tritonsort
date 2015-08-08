#!/usr/bin/env python

import bottle, os, sys, argparse, requests, redis, json, jinja2
from plumbum import local
import utils

HDFS_PATH_MAP_KEY = "hdfs:location_path_map"
HDFS_NODES_KEY = "hdfs:nodes"
HDFS_TOPLEVEL_DIRS_KEY = "hdfs:top_level_dirs"
HDFS_NEXT_TOPLEVEL_DIR_KEY = "hdfs_proxy:next_top_level_dir"

redis_host = None
redis_port = None
redis_db = None

hdfs_host = None
hdfs_port = None

SUPPORTED_OPS = ["OPEN", "CREATE", "DELETE", "APPEND", "GETFILESTATUS",
                 "GETFILECHECKSUM", "LISTSTATUS", "MKDIRS", "SETREPLICATION"]

jinja_env = jinja2.Environment(loader=jinja2.FileSystemLoader(
        os.path.dirname(os.path.abspath(__file__))), trim_blocks=True)

def new_redis_client():
    global redis_host, redis_port, redis_db

    return redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db)

@bottle.error(501)
def unimplemented_error(error):
    return ""

def possible_path_shards(path, redis_client):
    # Get the list of all top-level directories
    top_level_dirs = redis_client.smembers(HDFS_TOPLEVEL_DIRS_KEY)

    possible_shards = map(
        lambda x: x + path, top_level_dirs)

    return possible_shards

def webhdfs_url(path):
    if path != "/":
        path = path.strip('/')

    url = "http://%s:%d/webhdfs/v1/%s" % (hdfs_host, hdfs_port, path)

    return url

def handle_list_request(path, redis_client):
    possible_dir_shards = possible_path_shards(path, redis_client)

    dir_listing = {
        "FileStatuses" : {
            "FileStatus" : []
            }
        }

    not_found_count = 0

    directories = {}

    for dirname in possible_dir_shards:
        request_url = webhdfs_url(dirname)

        response = requests.get(
            request_url, allow_redirects=False, params={"op" : "LISTSTATUS"},
            config={"trust_env" : False})

        if response.status_code == 200:
            response_json = json.loads(response.content)
            file_statuses = response_json["FileStatuses"]["FileStatus"]

            for file_status in file_statuses:
                if file_status["type"] == "DIRECTORY":
                    cur_dirname = file_status["pathSuffix"]

                    if cur_dirname not in directories:
                        directories[cur_dirname] = file_status
                else:
                    file_status["proxyPath"] = dirname
                    dir_listing["FileStatuses"]["FileStatus"].append(
                        file_status)

        elif response.status_code == 404:
            not_found_count += 1

    dir_listing["FileStatuses"]["FileStatus"].extend(
        directories.values())

    if not_found_count == len(possible_dir_shards):
        exception = {
            "RemoteException":
                {
                "exception"    : "FileNotFoundException",
                "javaClassName": "java.io.FileNotFoundException",
                "message"      : "Directory does not exist: "
                }
            }
        raise bottle.HTTPResponse(status=404, output=json.dumps(exception))

    return json.dumps(dir_listing)

def handle_mkdirs_request(path, redis_client):
    dir_shards = possible_path_shards(path, redis_client)

    creation_succeeded = True
    response = json.dumps({ "boolean" : True })

    for dirname in dir_shards:
        url = webhdfs_url(dirname)

        mkdir_request = requests.put(
            url, params={"op" : "MKDIRS"}, config={"trust_env" : False},
            allow_redirects=False)

        if mkdir_request.status_code != 200:
            creation_succeeded = False
            response = response.content

    if creation_succeeded:
        status = 200
        redis_client.hsetnx(HDFS_PATH_MAP_KEY, path, dir_shards[0])
    else:
        status = 403

    http_response = bottle.HTTPResponse(
        output=response, status=status,
        header={"Content-Type" : "application/json"})

    raise http_response

def handle_delete_request(path, redis_client):
    possible_shards = possible_path_shards(path, redis_client)

    delete_params = {
        "op" : "DELETE"
        }

    if "recursive" in bottle.request.query:
        delete_params["recursive"] = bottle.request.query["recursive"]

        keys_to_delete = filter(lambda x: x.find(path) == 0,
                                redis_client.hkeys(HDFS_PATH_MAP_KEY))

        if len(keys_to_delete) > 0:
            redis_client.hdel(HDFS_PATH_MAP_KEY, *keys_to_delete)
    else:
        redis_client.hdel(HDFS_PATH_MAP_KEY, path)

    expected_status_codes = [200, 404]

    for shard in possible_shards:
        url = webhdfs_url(shard)
        delete_response = requests.delete(
            url, config={"trust_env" : False}, params=delete_params)

        if delete_response.status_code not in expected_status_codes:
            print >>sys.stderr, (
                "Unexpected status code %d from DELETE on %s" % (
                    delete_response.status_code, shard))
            raise bottle.HTTPResponse(
                output = delete_response.content,
                status = delete_response.status_code,
                header = { "Content-Type" : "application/json" })

    raise bottle.HTTPResponse(
        output=json.dumps({"boolean" : True}),
        status=200,
        header = { "Content-Type" : "application/json" })

def handle_request(path, method, redis_client):
    path = '/' + path

    op = bottle.request.query["op"]

    if op not in SUPPORTED_OPS:
        print >>sys.stderr, "Unsupported operation %s" % (op)
        raise bottle.HTTPResponse(status=501)

    # If this path is unproxied, replace it with the proxied version of the path
    proxied_path = unproxied_to_proxied_path(path, redis_client)

    if proxied_path is not None:
        # Make sure there is a mapping between this unproxied path and the
        # proxied version
        redis_client.hsetnx(HDFS_PATH_MAP_KEY, proxied_path, path)
        path = proxied_path

    # Certain operations should operate on the proxied version of the path
    # only; we'll short-circuit on these if needed
    if op == "LISTSTATUS" and method == "get":
        return handle_list_request(path, redis_client)
    elif op == "MKDIRS" and method == "put":
        return handle_mkdirs_request(path, redis_client)
    elif op == "DELETE" and method == "delete":
        return handle_delete_request(path, redis_client)

    # Translate the proxied path to its unproxied equivalent

    if redis_client.hexists(HDFS_PATH_MAP_KEY, path):
        unproxied_path = redis_client.hget(HDFS_PATH_MAP_KEY, path)
    elif op == "GETFILESTATUS":
        # If we're trying to stat a logical path that doesn't exist, we should
        # tell the user it doesn't exist rather than create a new mapping

        not_found_exception = {
            "RemoteException" : {
                "exception" : "FileNotFoundException",
                "javaClassName" : "java.io.FileNotFoundException",
                "message" : "File does not exist: %s" % (path)
                }
            }

        raise bottle.HTTPResponse(
            output=json.dumps(not_found_exception), status=404)
    else:
        unproxied_path = rewrite_path(path, redis_client)
        redis_client.hsetnx(HDFS_PATH_MAP_KEY, path, unproxied_path)

    # Set up the query params portion of the new URL
    new_url_params = {}

    for key, val in bottle.request.query.items():
        new_url_params[str(key)] = str(val)

    # Create a new URL based on the unproxied version of the path
    new_url = webhdfs_url(unproxied_path)

    response = requests.request(
        method, new_url, config={"trust_env" : False},
        params = new_url_params,
        allow_redirects=False, headers={"Content-Length" : "0"})

    # wsgiref doesn't allow me to return "hop-by-hop" headers, so I have to
    # strip them from the result (see
    # http://www.w3.org/Protocols/rfc2616/rfc2616-sec13.html, Section 13.5.1)
    sanitized_headers = {}

    for header, value in response.headers.items():
        if header not in [
            "connection", "keep-alive", "proxy-authenticate",
            "proxy-authorization", "te", "trailers", "transfer-encoding",
            "upgrade"]:

            sanitized_headers[header] = value

    http_response = bottle.HTTPResponse(
        output=response.content, status=response.status_code,
        header=sanitized_headers)

    raise http_response

@bottle.put('/webhdfs/v1/<path:path>')
def handle_put(path):
    redis_client = new_redis_client()
    return handle_request(path, "put", redis_client)

@bottle.get('/webhdfs/v1/<path:path>')
def handle_get(path):
    redis_client = new_redis_client()
    return handle_request(path, "get", redis_client)

@bottle.post('/webhdfs/v1/<path:path>')
def handle_post(path):
    redis_client = new_redis_client()
    return handle_request(path, "post", redis_client)

@bottle.delete('/webhdfs/v1/<path:path>')
def handle_delete(path):
    redis_client = new_redis_client()
    return handle_request(path, "delete", redis_client)

@bottle.route('/status')
def status():
    redis_client = new_redis_client()

    path_map = redis_client.hgetall(HDFS_PATH_MAP_KEY)
    template = jinja_env.get_template("hdfs_proxy_status.jinja2")
    return template.render(paths = path_map)

def abbreviate_disk_path(path):
    if path.find("cciss") != -1:
        return path.split('/')[4]
    else:
        sys.exit("Don't know how to abbreviate disk path '%s' (and HDFS "
                 "probably doesn't either!)" % (path))

def get_ipv4_address(host, redis_client):
    address = redis_client.hget("ipv4_address", host)

    if address is None:
        sys.exit("Can't find IPv4 address for host '%s'" % (host))

    return address

def rewrite_path(path, redis_client):
    info = redis_client.hget(HDFS_PATH_MAP_KEY, path)

    if info is None:
        top_level_directory = None

        # Choose new path
        with redis_client.pipeline() as pipe:
            while top_level_directory is None:
                try:
                    pipe.watch(HDFS_NEXT_TOPLEVEL_DIR_KEY)
                    pipe.watch(HDFS_TOPLEVEL_DIRS_KEY)
                    toplevels = list(pipe.smembers(HDFS_TOPLEVEL_DIRS_KEY))
                    toplevels.sort()
                    current_toplevel = int(pipe.get(HDFS_NEXT_TOPLEVEL_DIR_KEY))
                    top_level_directory = toplevels[current_toplevel]

                    pipe.multi()
                    pipe.set(HDFS_NEXT_TOPLEVEL_DIR_KEY,
                             (current_toplevel + 1) % len(toplevels))
                    pipe.execute()
                except redis.WatchError:
                    top_level_directory = None

        if path[0] == '/':
            new_path = "%s%s" % (top_level_directory, path)
        else:
            new_path = "%s/%s" % (top_level_directory, path)

        return new_path
    else:
        return info

def mkdirs(dirname):
    url = webhdfs_url(dirname)

    mkdir_request = requests.put(
        url, params={"op" : "MKDIRS"}, config={"trust_env" : False},
        allow_redirects=False)

    return (mkdir_request.status_code, mkdir_request.content)

def build_toplevel_directories(redis_client):
    nodes = redis_client.lrange(HDFS_NODES_KEY, 0, -1)

    for node in nodes:
        node_ip = redis_client.hget("ipv4_address", node)

        disks = list(redis_client.smembers("node_hdfs_disks:%s" % (node)))
        disks.sort()

        for disk_id, disk in enumerate(disks):
            toplevel_path = "/%s/%d" % (node_ip, disk_id)

            print "Creating toplevel directory %s ..." % (toplevel_path)
            (status, content) = mkdirs(toplevel_path)

            if status != 200:
                sys.exit("Can't create toplevel directory %s: MKDIRS request "
                         "returned code %d (%s)" %
                         (toplevel_path, status, content))

            else:
                redis_client.sadd(HDFS_TOPLEVEL_DIRS_KEY, toplevel_path)


def build_hdfs_redis_state(redis_client):
    # Reset the mapping from logical paths to HDFS paths before launching so we
    # don't have any stale data

    redis_client.delete(HDFS_PATH_MAP_KEY)
    redis_client.delete(HDFS_NODES_KEY)
    redis_client.delete(HDFS_TOPLEVEL_DIRS_KEY)
    redis_client.set(HDFS_NEXT_TOPLEVEL_DIR_KEY, "0")

    # For now, all nodes are HDFS clients
    hdfs_nodes = redis_client.smembers("nodes")

    if len(hdfs_nodes) == 0:
        sys.exit("Can't find any nodes from which to derive a list of HDFS "
                 "nodes")

    redis_client.rpush(HDFS_NODES_KEY, *hdfs_nodes)

    # Build a list of (and create, if they don't exist) all top-level
    # directories

    build_toplevel_directories(redis_client)

    build_hdfs_path_map("/", 0, redis_client)

def unproxied_to_proxied_path(file_path, redis_client):
    """If this path is a path that has been translated by the proxy, return the
    logical path to which this path corresponds. Otherwise, return None
    """

    top_level_dirs = redis_client.smembers(HDFS_TOPLEVEL_DIRS_KEY)

    for dirname in top_level_dirs:
        if file_path.find(dirname) == 0:
            proxied_path = file_path[len(dirname):]

            if len(proxied_path) == 0:
                proxied_path = '/'

            return proxied_path

    return None

def build_hdfs_path_map(dirname, depth, redis_client):
    print "Populating path map for '%s' ..." % (dirname)

    list_status_url = webhdfs_url(dirname)

    response = requests.get(
        list_status_url, params = {"op" : "LISTSTATUS"},
        config={"trust_env" : False}, allow_redirects=False)

    if response.status_code != 200:
        sys.exit("Could not list directory for %s: error %d" %
                 (dirname, response.status_code))

    file_statuses = json.loads(response.content)

    for file_status in file_statuses["FileStatuses"]["FileStatus"]:
        file_path = os.path.join(dirname, file_status["pathSuffix"])
        logical_path = unproxied_to_proxied_path(file_path, redis_client)

        # Insert mapping for file
        if logical_path is not None:
            redis_client.hsetnx(HDFS_PATH_MAP_KEY, logical_path, file_path)

        if file_status["type"] == "DIRECTORY":
            # Recursively evaluate directories looking for files
            build_hdfs_path_map(file_path, depth + 1, redis_client)

def main():
    global hdfs_host, hdfs_port, redis_host, redis_port, redis_db

    parser = argparse.ArgumentParser(
        description="WebHDFS proxy that re-writes file requests to facilitate "
        "increased I/O parallelism")
    parser.add_argument(
        "hdfs_namenode", help="the host:port of the HDFS namenode for which "
        "this script will serve as a proxy")

    utils.add_redis_params(parser)

    args = parser.parse_args()

    redis_host = args.redis_host
    redis_port = args.redis_port
    redis_db = args.redis_db

    hdfs_namenode_parts = filter(
        lambda x: len(x) > 0, args.hdfs_namenode.split(':'))

    if len(hdfs_namenode_parts) == 2:
        hdfs_host, hdfs_port = hdfs_namenode_parts
        hdfs_port = int(hdfs_port)
    else:
        hdfs_host = hdfs_namenode_parts[0]
        hdfs_port = 50070

    print "Proxying %s:%d" % (hdfs_host, hdfs_port)

    redis_client = new_redis_client()

    # Reconstruct the path mapping from the existing state of HDFS
    build_hdfs_redis_state(redis_client)

    bottle.run(host='0.0.0.0', port=5000, server="paste")
    bottle.debug(True)

if __name__ == "__main__":
    sys.exit(main())
