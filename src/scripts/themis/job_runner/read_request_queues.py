#!/usr/bin/env python

import os, sys, argparse, redis, json

SCRIPT_DIR = os.path.abspath(os.path.dirname(__file__))

sys.path.append(os.path.abspath(os.path.join(SCRIPT_DIR, os.pardir)))

import utils

def readable_read_request(read_request):
    json_req = json.loads(read_request)

    req_type = json_req["type"]
    if req_type == 0:
        json_req["job_ids"] = ','.join(map(str, json_req["job_ids"]))
        return ("Read %(path)s for job(s) %(job_ids)s "
                "(%(length)dB @ offset %(offset)d)") % json_req
    elif req_type == 1:
        if "job_ids" in json_req:
            json_req["job_ids"] = ','.join(map(str, json_req["job_ids"]))
        else:
            json_req["job_ids"] = "???"
        return ("Halt job(s) %(job_ids)s") % (json_req)
    else:
        return read_request


def list_read_queues(redis_client, host, read_queues):
    print "%s:" % (host)

    for read_queue in read_queues:
        read_queue_chunks = read_queue.split(':')
        worker_name = read_queue_chunks[2]
        worker_id = read_queue_chunks[3]

        print "%s %s " % (worker_name, worker_id),

        read_requests = redis_client.lrange(read_queue, 0, -1)

        print "(%d element(s) in queue):" % (len(read_requests))

        for i, read_request in enumerate(read_requests):
            print "% 5d. %s" % (i + 1, readable_read_request(read_request))
        print ""

def flush_read_queues(redis_client, read_queues):
    for read_queue in read_queues:
        redis_client.delete(read_queue)

def read_request_queues(redis_host, redis_port, redis_db, directive):
    redis_client = redis.StrictRedis(
        host=redis_host, port=redis_port, db=redis_db)

    for host in redis_client.smembers("nodes"):
        ip_address = redis_client.hget("ipv4_address", host)

        read_queues = list(
            redis_client.smembers("read_requests:%s" % (ip_address)))
        read_queues.sort()

        if directive == "list":
            list_read_queues(redis_client, host, read_queues)
        elif directive == "flush":
            flush_read_queues(redis_client, read_queues)

def main():
    parser = argparse.ArgumentParser(
        description="dump the contents of the coordinator's read request "
        "queues")
    utils.add_redis_params(parser)

    parser.add_argument("directive", choices=["list", "flush"], help="specify "
                        "which action to perform on read request queues")

    args = parser.parse_args()

    return read_request_queues(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
