INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/RecurseCCFiles.cmake")
INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/UseWorker.cmake")

RECURSE_CC_FILES(common)

USE_WORKER(benchmarks/mallocbench mallocworker)

ADD_LIBRARY(mallocbench_workers STATIC ${common_Files}
  ${benchmarks/mallocbench_mallocworker_WorkerFiles}
)
TARGET_LINK_LIBRARIES(mallocbench_workers tritonsort_core tritonsort_common)

ADD_EXECUTABLE(mallocbench main.cc)
TARGET_LINK_LIBRARIES(mallocbench mallocbench_workers)
