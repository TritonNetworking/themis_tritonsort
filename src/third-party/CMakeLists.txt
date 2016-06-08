PROJECT(ThirdParty)

ADD_SUBDIRECTORY(googletest)

ADD_LIBRARY(murmurhash
  murmurhash/MurmurHash3.cpp)
