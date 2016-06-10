INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/UseWorker.cmake")

USE_WORKER(benchmarks/mixediobench demux)

ADD_LIBRARY(mixediobench_workers STATIC
  ${benchmarks/mixediobench_demux_WorkerFiles})

TARGET_LINK_LIBRARIES(mixediobench_workers tritonsort_core
  tritonsort_common mapreduce_workers storagebench_workers
  ${MEMORY_MGMT_LINK_LIBRARY})

ADD_EXECUTABLE(mixediobench main.cc)
TARGET_LINK_LIBRARIES(mixediobench mixediobench_workers)

IF(DEFINED MEMORY_MGMT_CXX_FLAG)
  SET_TARGET_PROPERTIES(mixediobench PROPERTIES COMPILE_FLAGS
    ${MEMORY_MGMT_CXX_FLAG})
  IF(${MEMORY_MANAGER} STREQUAL "jemalloc")
    # Set jemalloc preprocessor define for includes.
    TARGET_COMPILE_DEFINITIONS(mixediobench PRIVATE USE_JEMALLOC=1)
  ENDIF(${MEMORY_MANAGER} STREQUAL "jemalloc")
ENDIF(DEFINED MEMORY_MGMT_CXX_FLAG)

