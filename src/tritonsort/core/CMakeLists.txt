FILE(GLOB CoreFiles "*.cc")

LINK_DIRECTORIES(${Boost_LIBRARY_DIRS})

ADD_LIBRARY(tritonsort_core STATIC ${CoreFiles})
TARGET_LINK_LIBRARIES(tritonsort_core ${Boost_LIBRARIES}
  ${JSONCPP_LIBRARY} ${pthread_LIBRARY} ${rt_LIBRARY} ${aio_LIBRARY} murmurhash
  ${YAMLCPP_LIBRARY})
