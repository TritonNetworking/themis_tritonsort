INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/UseWorker.cmake")

USE_WORKER(benchmarks/networkbench generator)

ADD_LIBRARY(networkbench_workers STATIC
  ${benchmarks/networkbench_generator_WorkerFiles})

TARGET_LINK_LIBRARIES(networkbench_workers tritonsort_core
  tritonsort_common mapreduce_workers)

ADD_EXECUTABLE(networkbench main.cc)
TARGET_LINK_LIBRARIES(networkbench networkbench_workers
  ${MEMORY_MGMT_LINK_LIBRARY})

IF(DEFINED MEMORY_MGMT_CXX_FLAG)
SET_TARGET_PROPERTIES(networkbench PROPERTIES COMPILE_FLAGS
  ${MEMORY_MGMT_CXX_FLAG})
ENDIF(DEFINED MEMORY_MGMT_CXX_FLAG)
