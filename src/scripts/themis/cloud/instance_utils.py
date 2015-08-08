import amazon.instance_utils as amazon_utils
import google.instance_utils as google_utils
from plumbum.cmd import fping
from plumbum.commands.processes import ProcessExecutionError

def get_instance_names(provider, cluster_ID, zone):
    if provider == "amazon":
        return amazon_utils.get_instance_names(cluster_ID, zone)
    elif provider == "google":
        return google_utils.get_instance_names(cluster_ID, zone)
    else:
        raise ValueError("Illegal provider %s. Must be amazon or google.")

def get_instance_addresses(provider, cluster_ID, zone):
    if provider == "amazon":
        return amazon_utils.get_instance_addresses(cluster_ID, zone)
    elif provider == "google":
        return google_utils.get_instance_addresses(cluster_ID, zone)
    else:
        raise ValueError("Illegal provider %s. Must be amazon or google.")

def get_cluster_status(provider, cluster_ID, zone):
    if provider == "amazon":
        results = amazon_utils.get_cluster_status(cluster_ID, zone)
    elif provider == "google":
        results = google_utils.get_cluster_status(cluster_ID, zone)
    else:
        raise ValueError("Illegal provider %s. Must be amazon or google.")

    master_connectable = False
    if results["master"] != None:
        try:
            # Check for connectability using the external hostname.
            master_hostname = results["master"][1]
            live_master = fping["-a"]["-r1"][master_hostname]()
            if len(live_master) > 0:
                master_connectable = True
        except ProcessExecutionError as e:
            # Not connectable
            pass

    results["master_connectable"] = master_connectable

    return results
