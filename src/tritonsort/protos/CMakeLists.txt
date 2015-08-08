INCLUDE(${TritonSort_SOURCE_DIR}/cmake_tools/GenProtos.cmake)

FILE(GLOB PROTO_FILES "*.proto")
GEN_PROTOS(tritonsort_protos ${CMAKE_CURRENT_BINARY_DIR}/cpp ${TritonSort_SOURCE_DIR}/../scripts/tritonsort/protos ${PROTO_FILES})
