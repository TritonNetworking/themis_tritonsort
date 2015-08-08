#!/usr/bin/env python

import os, sys, argparse, struct, time, subprocess

sys.path.append(os.path.join(os.path.abspath(
            os.path.dirname(__file__)), os.pardir))

from disks.dfs.node_dfs import dfs_get_local_paths, dfs_mkdir

JAVA_HOME = os.getenv('JAVA_HOME', "/mnt/dcswitch/seed/java/jdk1.6.0_19/")

def generate_cloudBurst_input(
  num_splits, num_input_disks, file_offset_index,
  reference_file_path, reference_file_total_sequences, query_file_path,
  query_file_total_sequences, output_file_path, host_input_directory,
  executable_path):
    """
      Generate binary coded input files for cloudburst MapReduce job. The
      script takes raw sequence file as input (in FASTA FORMAT) and split
      and binary code it and copies file to intermediate directory.

      ConvertFastaForThemis.jar splits and binary code raw sequence file.
      Jar file (split n binary code process) is executed on master node,
      with file_offset_index = 0. Rest of experiment-nodes sleep till file
      is being splitted. Once split process is complete each host copies
      some subset of intermediate file to local directory.
    """
    # Create directory on DFS (recursively creating sub-directories) if it
    # doesn't already exist
    print "exec path", executable_path, "num_splits", str(num_splits),
    print  "output_file_path", output_file_path, "num disk",num_input_disks,
    print  "file_offset_index", file_offset_index

    dfs_mkdir(host_input_directory, True)
    equal_size = 1

    dummy_file = os.path.join(output_file_path, "dummy")
    if not os.path.exists(dummy_file):
        # If the dummy file does not exist, either wait for it if you are a
        # slave node, or run the converter jar and create the dummy file if you
        # are the master node.

        if file_offset_index != 0:
            # check if dummy file exists which indicates master node
            # finished off split process
            dummy_file = os.path.join(output_file_path, "dummy")
            while os.path.exists(dummy_file) == False:
                time.sleep(1)
        else:
            file_names = [(reference_file_path, reference_file_total_sequences),
                          (query_file_path, query_file_total_sequences)]
            for (file_path, seq_size) in file_names:
                output_filename = "output_" + os.path.basename(file_path)
                output_file = os.path.join(output_file_path, output_filename)
                print file_path, output_filename
                cloudburst_file_converter_hosts_cmd = (
                    "%s/bin/java -jar %s %s %s %s %s %s")% (JAVA_HOME,
                        executable_path, file_path, output_file, equal_size,
                        seq_size, num_splits)

                print cloudburst_file_converter_hosts_cmd
                running_cmd = subprocess.Popen(
                                           cloudburst_file_converter_hosts_cmd,
                                           universal_newlines=True,
                                           shell=True,
                                           stdout=subprocess.PIPE,
                                           stderr=subprocess.PIPE)
                (stdout, stderr) = running_cmd.communicate()

                if running_cmd.returncode != 0:
                    sys.exit("Command '%s' failed: %s" %
                        (' '.join(cloudburst_file_converter_hosts_cmd), stderr))
            # create a dummy file
            f = open(dummy_file, 'w')
            f.close()
    # Get the local disks corresponding to that directory
    local_paths = dfs_get_local_paths(host_input_directory)[:num_input_disks]

    print local_paths

    for (local_path_id, local_path) in enumerate(local_paths):
        if os.path.exists(local_path):
          #  scp the input files to local disks
          fileIndex = local_path_id + num_input_disks*file_offset_index+1
          #  copy all the files which match of form *1,2..25,35..
          input_file = output_file_path + "output_*[a-zA-z]"+ str(fileIndex)
          cloudburst_scp_input_file_command = "scp %s %s/" %(
            input_file, local_path)
          print cloudburst_scp_input_file_command
          running_cmd = subprocess.Popen(cloudburst_scp_input_file_command,
                                         universal_newlines=True,
                                         shell=True,
                                         stdout=subprocess.PIPE,
                                         stderr=subprocess.PIPE)
          (stdout, stderr) = running_cmd.communicate()
          if running_cmd.returncode != 0:
              sys.exit("Command '%s' failed: %s" % (
                  ' '.join(cloudburst_scp_input_file_command), stderr))

def main():
    parser = argparse.ArgumentParser(description="generate an input file per "
                                     "disk in the provided DFS directory "
                                     "Inputs to the functions are files in "
                                     "Fasta format and it generates byte "
                                     "byte convertable files")
    parser.add_argument('--num_splits', "-s", type=int,
                        help="number of disks to "
                        "use as input disks",
                        default=0)
    parser.add_argument('--num_input_disks', "-n",
                        help="number of disks to use as input disks",
                        type=int, default=8)
    parser.add_argument('reference_file_path',
                        help ="Location of Reference input file")
    parser.add_argument('reference_file_total_sequences', type=int,
                        help ="Total number of sequence in reference file")
    parser.add_argument('query_file_path', type=str,
                        help ="Input file directory")
    parser.add_argument('query_file_total_sequences', type=int,
                        help ="Total number of sequences in query file")
    parser.add_argument('output_file_path',type=str,
                        help="host node directory where output files be"
                         "stored. Directory will be created if it doesnt "
                         "already exist")
    parser.add_argument('file_offset_index', type=int,
                        help="Index files for the host")
    parser.add_argument('host_input_directory', type=str,
                        help="experiment node directory where input files will"
                         "be copied to ")
    parser.add_argument('executable_path', type=str,
                        help="location of executable/jar file")
    args = parser.parse_args()

    generate_cloudBurst_input(**vars(args))

if __name__ == "__main__":
    main()
