import json, plumbum

def _instance_has_metadata(instance, metadata_key, metadata_value):
    metadata_key = str(metadata_key)
    metadata_value = str(metadata_value)

    metadata = instance["Tags"]
    metadata = [item for item in metadata if item["Key"] == metadata_key and \
                item["Value"] == metadata_value]

    return (len(metadata) > 0)

def _filter_instances(instances, node_type, status):
    instances = [instance for instance in instances \
                 if _instance_has_metadata(instance, "NODE_TYPE", node_type)]
    instances = [instance for instance in instances \
                 if _instance_has_metadata(instance, "STATUS", status)]

    return instances

def _addresses(instances):
    # Return a tuple of (internal, external) addresses
    return [(instance["PrivateIpAddress"], instance["PublicDnsName"])\
            for instance in instances]

def _names(instances):
    # EC2 instances are addressed by ID rather than instance name.
    return [instance["InstanceId"] for instance in instances]

def _list_instances(cluster_ID, zone):
    aws = plumbum.local["aws"]
    instances = aws["--profile"]["themis"]["ec2"]["describe-instances"]\
                ["--filters"]["Name=instance-state-name,Values=running"]\
                ["Name=tag:CLUSTER_ID,Values=%s" % cluster_ID]()
    instances = json.loads(instances)
    # Flatten out reservations
    return [instance for reservation in instances["Reservations"]\
            for instance in reservation["Instances"]]

def get_instance_names(cluster_ID, zone):
    return _names(_list_instances(cluster_ID, zone))

def get_instance_addresses(cluster_ID, zone):
    return _addresses(_list_instances(cluster_ID, zone))

def get_cluster_status(cluster_ID, zone):
    instances = _list_instances(cluster_ID, zone)

    booting_slaves = _addresses(
        _filter_instances(instances, "slave", "Booting"))
    online_slaves = _addresses(
        _filter_instances(instances, "slave", "Online"))

    booting_masters = _addresses(
        _filter_instances(instances, "master", "Booting"))
    online_masters = _addresses(
        _filter_instances(instances, "master", "Online"))

    master = None
    if len(booting_masters) > 0:
        master = booting_masters[0]
    elif len(online_masters) > 0:
        master = online_masters[0]

    results = {}
    results["booting_slaves"] = booting_slaves
    results["online_slaves"] = online_slaves
    results["booting_masters"] = booting_masters
    results["online_masters"] = online_masters
    results["master"] = master

    results["num_booting_slaves"] = len(booting_slaves)
    results["num_online_slaves"] = len(online_slaves)
    results["num_booting_masters"] = len(booting_masters)
    results["num_online_masters"] = len(online_masters)

    return results
