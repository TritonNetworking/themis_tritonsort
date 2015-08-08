dofile("PATH/src/tritonsort/mapreduce/config.default.mapreduce")

MEM_SIZE = 25769803776
ALLOCATOR_CAPACITY = math.floor(0.90 * MEM_SIZE)

CORES_PER_NODE = 16
THREAD_CPU_POLICY="CUSTOM"
THREAD_POLICY_reader              = "0000000100000000"
THREAD_POLICY_writer              = "0000000000000001"
