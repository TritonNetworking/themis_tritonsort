#ADD_SUBDIRECTORY(partitionbench)
# Disable endianconversionbench by default because it won't build unless you
# have htobe64
#ADD_SUBDIRECTORY(endianconversionbench)
ADD_SUBDIRECTORY(mallocbench)
ADD_SUBDIRECTORY(mixediobench)
ADD_SUBDIRECTORY(networkbench)
ADD_SUBDIRECTORY(radixsortbench)
ADD_SUBDIRECTORY(storagebench)
