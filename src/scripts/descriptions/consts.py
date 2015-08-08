# Filenames
STRUCTURE_FILENAME = "structure.json"
STAGE_INFO_FILENAME = "stages.json"

# Common keys
TYPE_KEY = "type"

# Keys for structure info object
PHASES_KEY = "phases"

# Keys for phase information object
STAGES_KEY = "stages"
STAGE_TO_STAGE_KEY = "stage-to-stage connections"
NETWORK_CONN_KEY = "network connections"

# Keys for stage information object
COUNT_PARAM_KEY = "count_param"
INPUT_SIZE_KEY = "input_size"

# Keys for edge object
SRC_KEY = "src"
DEST_KEY = "dest"

# Stage info keys
ACTIVE_NODES_KEY = "active_nodes"

# Keys for input size info object
INPUT_SIZE_REF_TYPE = "ref"
INPUT_SIZE_REGEX_TYPE = "regex"
INPUT_SIZE_PARAM_TYPE = "param"
INPUT_SIZE_STAT_TYPE = "stat"
INPUT_SIZE_PARAM_NAME_KEY = "param_name"
INPUT_SIZE_STAT_NAME_KEY = "stat_name"
INPUT_SIZE_STAT_SUBNAME_KEY = "stat_subname"
INPUT_SIZE_STAGE_KEY = "stage"
INPUT_SIZE_REGEX_KEY = "regex"

# Constants for network connections
CONN_TYPE_KEY = "type"
CONN_TYPE_MANY_TO_MANY = "many-to-many"
CONN_TYPE_MANY_TO_ONE = "many-to-one"
CONN_TYPE_ONE_TO_MANY = "one-to-many"
CONN_SRC_NODE = "src_node"
CONN_DEST_NODE = "dest_node"
CONN_BYPASS_LOCALHOST = "bypass_localhost"

# Constants for constraints
MAX_WORKERS_KEY = "max_workers"
MAX_RATE_PER_WORKER_KEY = "max_rate_per_worker"
IO_BOUND_KEY = "io_bound"
