import json, plumbum

def _instance_has_metadata(instance, metadata_key, metadata_value):
    metadata_key = str(metadata_key)
    metadata_value = str(metadata_value)

    metadata = instance["metadata"]["items"]
    metadata = [item for item in metadata if item["key"] == metadata_key and \
                item["value"] == metadata_value]

    return (len(metadata) > 0)

def _filter_instances(instances, node_type, status):
    instances = [instance for instance in instances \
                 if _instance_has_metadata(instance, "NODE_TYPE", node_type)]
    instances = [instance for instance in instances \
                 if _instance_has_metadata(instance, "STATUS", status)]

    return instances

def _addresses(instances):
    # Return a tuple of (internal, external) addresses
    return [(instance["networkInterfaces"][0]["networkIP"],
             instance["networkInterfaces"][0]["accessConfigs"][0]["natIP"])\
            for instance in instances]

def _names(instances):
    return [instance["name"] for instance in instances]

def _list_instances(cluster_ID, zone):
    gcloud = plumbum.local["gcloud"]
    instances = gcloud["compute"]["instances"]["list"]["--zone"][zone]\
                ["--format"]["json"]()
    instances = json.loads(instances)

    instances = [instance for instance in instances\
            if instance["status"] == "RUNNING"]
    return [instance for instance in instances \
            if _instance_has_metadata(instance, "CLUSTER_ID", cluster_ID)]

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
