CMAKE_MINIMUM_REQUIRED(VERSION 2.6)
PROJECT(PartitionBenchmark)

INCLUDE_DIRECTORIES(${TritonSort_SOURCE_DIR})

ADD_EXECUTABLE(partitionbench partitionbenchmark.cc)
TARGET_LINK_LIBRARIES(partitionbench tritonsort_core mapreduce_workers)
