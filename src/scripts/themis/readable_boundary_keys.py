#!/usr/bin/env python

import sys, argparse, struct, binascii

def read_keys(file_bytes, start_offset):
    offset = start_offset

    (keys_length,) = struct.unpack_from(">Q", file_bytes, offset)
    offset += 8 + keys_length

    (num_key_infos,) = struct.unpack_from(">Q", file_bytes, offset)
    offset += 8

    key_lengths = []

    for i in xrange(num_key_infos):
        (key_length,) = struct.unpack_from(">L", file_bytes, offset)
        offset += 4
        key_lengths.append(key_length)

    total_length = offset - start_offset

    keys = []

    offset = start_offset + 8

    for key_length in key_lengths:
        keys.append(
            (key_length,
             struct.unpack_from("<%ds" % (key_length), file_bytes, offset)[0]))

        offset += key_length

    return (keys, total_length)

def print_key(key, key_length):
    print ("(%d): " % (key_length)) + binascii.b2a_hex(key)

def readable_boundary_keys(filename):
    with open(filename, 'rb') as fp:
        file_bytes = fp.read()

    offset = 0

    (global_keys, length) = read_keys(file_bytes, offset)
    print length
    offset += length

    print "Global keys:"

    for (key_length, key) in global_keys:
        print_key(key, key_length)

    (local_keys, length) = read_keys(file_bytes, offset)

    print "\nLocal keys:"

    for (key_length, key) in local_keys:
        print_key(key, key_length)

def main():
    parser = argparse.ArgumentParser(
        description="dump a readable representation of a saved boundary "
        "keys file")
    parser.add_argument("filename", help="the name of the file to read")
    args = parser.parse_args()
    return readable_boundary_keys(**vars(args))

if __name__ == "__main__":
    sys.exit(main())
