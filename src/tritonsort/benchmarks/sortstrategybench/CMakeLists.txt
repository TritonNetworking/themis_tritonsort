INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/UseWorker.cmake")

USE_WORKER(benchmarks/sortstrategybench timestamper)
USE_WORKER(benchmarks/sortstrategybench validator)

ADD_LIBRARY(sortstrategybench_workers STATIC
  ${benchmarks/sortstrategybench_timestamper_WorkerFiles}
  ${benchmarks/sortstrategybench_validator_WorkerFiles})

TARGET_LINK_LIBRARIES(sortstrategybench_workers tritonsort_core
  tritonsort_common mapreduce_workers)

ADD_EXECUTABLE(sortstrategybench main.cc)
TARGET_LINK_LIBRARIES(sortstrategybench sortstrategybench_workers)
