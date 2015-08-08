import os, yaml, shutil

def flattened_keys(dictionary, sort_function=None):
    for key, value in sorted(dictionary.items(), key=sort_function):
        if isinstance(value, dict):
            for index in flattened_keys(value):
                yield (key,) + index
        else:
            yield (key,)


class NestedDict(dict):
    def __init__(self, depth, leaf_function):
        assert depth > 0

        self.depth = depth
        self.leaf_function = leaf_function

        super(NestedDict, self).__init__()

    def __getitem__(self, key):
        if key in self:
            return self.get(key)

        if self.depth == 1:
            return self.setdefault(key, self.leaf_function())
        else:
            return self.setdefault(
                key, NestedDict(self.depth - 1, self.leaf_function))

def get_themisrc():
    themisrc = os.path.expanduser("~/.themisrc")
    if os.path.exists(themisrc):
        with open(themisrc) as fp:
            themisrc_config = yaml.load(fp)
            return themisrc_config

    return {}

def add_redis_params(parser):
    themisrc = get_themisrc()

    redis_host = "localhost"
    redis_port = 6379
    redis_db = 0

    default_str = "default"

    if "redis" in themisrc:
        default_str += " from ~/.themisrc"

        if "host" in themisrc["redis"]:
            redis_host = themisrc["redis"]["host"]
        if "port" in themisrc["redis"]:
            redis_port = themisrc["redis"]["port"]
        if "db" in themisrc["redis"]:
            redis_db = themisrc["redis"]["db"]

    parser.add_argument("--redis_host", help="the hostname that runs the "
                        "Themis redis server (%s: " % (default_str) +
                        "%(default)s)", default=redis_host)
    parser.add_argument("--redis_port", help="port number on which the redis "
                        "server is listening (%s: " % (default_str) +
                        "%(default)s)", default=redis_port, type=int)
    parser.add_argument("--redis_db", help="database number on the redis "
                        "server that Themis is using (%s: " % (default_str) +
                        "%(default)s)", default=redis_db, type=int)

def add_interfaces_params(parser):
    # Default to eth0
    interfaces = "eth0"

    default_str = "default"

    parser.add_argument(
        "--interfaces", help="comma delimited list of interfaces to be "
        "used for network communication (%s: " % (default_str) + "%(default)s)",
        default=interfaces, type=str)

def ssh_options():
    options = "-o PasswordAuthentication=no -o StrictHostKeyChecking=no"

    themisrc = get_themisrc()

    if "ssh" in themisrc and "key" in themisrc["ssh"]:
        options += ' -i "%s"' % (themisrc["ssh"]["key"])

    return options

def ssh_command():
    ssh_command = "ssh %s" % ssh_options()

    return ssh_command

def scp_command():
    scp_command = "scp %s" % ssh_options()

    return scp_command

def backup_if_exists(filename):
    if os.path.exists(filename):
        moved_to_backup = False
        backup_index = 1

        while not moved_to_backup:
            backup_name = "%s.old.%d" % (filename, backup_index)

            if not os.path.exists(backup_name):
                shutil.move(filename, backup_name)
                moved_to_backup = True
            else:
                backup_index += 1
