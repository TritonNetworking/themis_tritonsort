INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/UseWorker.cmake")

USE_WORKER(benchmarks/storagebench generator)
USE_WORKER(benchmarks/storagebench tagger)

ADD_LIBRARY(storagebench_workers STATIC
  ${benchmarks/storagebench_generator_WorkerFiles}
  ${benchmarks/storagebench_tagger_WorkerFiles})

TARGET_LINK_LIBRARIES(storagebench_workers tritonsort_core
  tritonsort_common mapreduce_workers)

ADD_EXECUTABLE(storagebench main.cc)
TARGET_LINK_LIBRARIES(storagebench storagebench_workers
  ${MEMORY_MGMT_LINK_LIBRARY})

IF(DEFINED MEMORY_MGMT_CXX_FLAG)
  SET_TARGET_PROPERTIES(storagebench PROPERTIES COMPILE_FLAGS
    ${MEMORY_MGMT_CXX_FLAG})
  IF(${MEMORY_MANAGER} STREQUAL "jemalloc")
    # Set jemalloc preprocessor define for includes.
    TARGET_COMPILE_DEFINITIONS(storagebench PRIVATE USE_JEMALLOC=1)
  ENDIF(${MEMORY_MANAGER} STREQUAL "jemalloc")
ENDIF(DEFINED MEMORY_MGMT_CXX_FLAG)
