# gensort and valsort Makefile
#
# Define USE_SUMP_PUMP as "1" to use the SUMP Pump library to make
# multithreaded versions of gensort and valsort.  Use any other value
# to make single-threaded versions.  SUMP Pump requires the pthread library.
#
USE_SUMP_PUMP=0
# use BITS=-m32 to force 32-bit build on a 64-bit platform
BITS=
CFLAGS=$(BITS) -O3 -Wall

all: gensort valsort

ifeq ($(USE_SUMP_PUMP),1)
# {
gensort: gensort.c rand16.o rand16.h sump.o
	gcc -g $(CFLAGS) -o gensort -DSUMP_PUMP gensort.c rand16.o sump.o -lz -lpthread -ldl -lrt

valsort: valsort.c rand16.o rand16.h sump.o
	gcc -g $(CFLAGS) -o valsort -DSUMP_PUMP valsort.c rand16.o sump.o -lz -lpthread -ldl -lrt

sump.o: sump.c sump.h
	gcc -g $(BITS) -Wall -Wno-char-subscripts -c -DSUMP_PUMP_NO_SORT sump.c
# }
else
# {
gensort: gensort.c rand16.o rand16.h
	gcc -g $(CFLAGS) -o gensort gensort.c rand16.o -lz

valsort: valsort.c rand16.o rand16.h
	gcc -g $(CFLAGS) -o valsort valsort.c rand16.o -lz
# }
endif

rand16.o: rand16.c rand16.h
	gcc -g $(CFLAGS) -c rand16.c
