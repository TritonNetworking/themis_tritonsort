import getpass
import socket

# User's username (for things like directory construction)
USERNAME = getpass.getuser()

HOSTNAME = socket.gethostname()

# Print header for status log messages
CHANNEL_STATUS_HEADER = "[STATUS]"

# Print header for statistics log messages
CHANNEL_STATISTIC_HEADER = "[STATISTIC]"

# Print header for parameter log messages
CHANNEL_PARAM_HEADER = "[PARAM]"

# Default thread-to-CPU policy
THREAD_CPU_POLICY = "DEFAULT"
