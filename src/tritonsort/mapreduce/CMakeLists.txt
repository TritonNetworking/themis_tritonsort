INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/RecurseCCFiles.cmake")
INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/UseWorker.cmake")
INCLUDE("${TritonSort_SOURCE_DIR}/cmake_tools/Findhiredis.cmake")

FIND_PACKAGE(CURL REQUIRED)

INCLUDE_DIRECTORIES(${TritonSort_SOURCE_DIR} ${CURL_INCLUDE_DIR})

RECURSE_CC_FILES(common)
RECURSE_CC_FILES(functions)
USE_WORKER(common coalescer)
USE_WORKER(common itemholder)
USE_WORKER(common receiver)
USE_WORKER(mapreduce boundarydecider)
USE_WORKER(mapreduce boundarydeserializer)
USE_WORKER(mapreduce boundaryscanner)
USE_WORKER(mapreduce buffercombiner)
USE_WORKER(mapreduce bytestreamconverter)
USE_WORKER(mapreduce chainer)
USE_WORKER(mapreduce mapper)
USE_WORKER(mapreduce merger)
USE_WORKER(mapreduce reader)
USE_WORKER(mapreduce reducer)
USE_WORKER(mapreduce sender)
USE_WORKER(mapreduce sorter)
USE_WORKER(mapreduce tupledemux)
USE_WORKER(mapreduce writer)

ADD_LIBRARY(mapreduce_workers STATIC ${common_Files}
  ${common_coalescer_WorkerFiles}
  ${common_itemholder_WorkerFiles}
  ${common_receiver_WorkerFiles}
  ${functions_Files}
  ${mapreduce_boundarydecider_WorkerFiles}
  ${mapreduce_boundarydeserializer_WorkerFiles}
  ${mapreduce_boundaryscanner_WorkerFiles}
  ${mapreduce_buffercombiner_WorkerFiles}
  ${mapreduce_bytestreamconverter_WorkerFiles}
  ${mapreduce_chainer_WorkerFiles}
  ${mapreduce_mapper_WorkerFiles}
  ${mapreduce_merger_WorkerFiles}
  ${mapreduce_reader_WorkerFiles}
  ${mapreduce_reducer_WorkerFiles}
  ${mapreduce_sender_WorkerFiles}
  ${mapreduce_sorter_WorkerFiles}
  ${mapreduce_tupledemux_WorkerFiles}
  ${mapreduce_writer_WorkerFiles}
  )

FIND_PACKAGE(OpenSSL REQUIRED)
FIND_PACKAGE(hiredis REQUIRED)

TARGET_LINK_LIBRARIES(mapreduce_workers tritonsort_core tritonsort_common
  ${OPENSSL_CRYPTO_LIBRARIES} ${CURL_LIBRARY} ${hiredis_LIBRARY})

# Create a version of MapReduce for phase zero/one, and another version for
# phase two
ADD_EXECUTABLE(mapreduce main.cc)
TARGET_LINK_LIBRARIES(mapreduce mapreduce_workers ${MEMORY_MGMT_LINK_LIBRARY})

ADD_EXECUTABLE(minute_sort minute_sort.cc)
TARGET_LINK_LIBRARIES(minute_sort mapreduce_workers ${MEMORY_MGMT_LINK_LIBRARY})

ADD_EXECUTABLE(daytona_minute_sort daytona_minute_sort.cc)
TARGET_LINK_LIBRARIES(daytona_minute_sort mapreduce_workers ${MEMORY_MGMT_LINK_LIBRARY})

IF(DEFINED MEMORY_MGMT_CXX_FLAG)
SET_TARGET_PROPERTIES(mapreduce PROPERTIES COMPILE_FLAGS
  ${MEMORY_MGMT_CXX_FLAG})
ENDIF(DEFINED MEMORY_MGMT_CXX_FLAG)

IF(DEFINED MEMORY_MGMT_CXX_FLAG)
SET_TARGET_PROPERTIES(minute_sort PROPERTIES COMPILE_FLAGS
  ${MEMORY_MGMT_CXX_FLAG})
ENDIF(DEFINED MEMORY_MGMT_CXX_FLAG)

IF(DEFINED MEMORY_MGMT_CXX_FLAG)
SET_TARGET_PROPERTIES(daytona_minute_sort PROPERTIES COMPILE_FLAGS
  ${MEMORY_MGMT_CXX_FLAG})
ENDIF(DEFINED MEMORY_MGMT_CXX_FLAG)

ADD_EXECUTABLE(mapreduce_phase_two main.cc)
TARGET_LINK_LIBRARIES(mapreduce_phase_two mapreduce_workers
  ${MEMORY_MGMT_PHASE_TWO_LINK_LIBRARY})

IF(DEFINED MEMORY_MGMT_PHASE_TWO_CXX_FLAG)
SET_TARGET_PROPERTIES(mapreduce_phase_two PROPERTIES COMPILE_FLAGS
  ${MEMORY_MGMT_PHASE_TWO_CXX_FLAG})
ENDIF(DEFINED MEMORY_MGMT_PHASE_TWO_CXX_FLAG)

# Copy defaults.yaml to out-of-source build if needed
CONFIGURE_FILE(${CMAKE_CURRENT_SOURCE_DIR}/defaults.yaml
  ${CMAKE_CURRENT_BINARY_DIR}/defaults.yaml COPYONLY)
