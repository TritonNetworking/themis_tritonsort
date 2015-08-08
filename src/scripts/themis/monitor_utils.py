import os, subprocess

def start_monitors(log_directory, hostname, vnstat_interface):
    monitor_processes = []
    monitor_output_files = []

    # Start sar, iostat, and vnstat logging.
    sar_cpu_output_file = open(
        os.path.join(log_directory, "sar-cpu-%s.out" % (hostname)), "w")
    command = ["sar", "1"]
    sar_cpu_process = subprocess.Popen(command, stdout=sar_cpu_output_file)
    monitor_processes.append(sar_cpu_process)
    monitor_output_files.append(sar_cpu_output_file)

    sar_memory_output_file = open(
        os.path.join(log_directory, "sar-memory-%s.out" % (hostname)), "w")
    command = ["sar", "-r", "1"]
    sar_memory_process = subprocess.Popen(
        command, stdout=sar_memory_output_file)
    monitor_processes.append(sar_memory_process)
    monitor_output_files.append(sar_memory_output_file)

    iostat_output_file = open(
        os.path.join(log_directory, "iostat-%s.out" % (hostname)), "w")
    command = ["iostat", "-mx", "5"]
    iostat_process = subprocess.Popen(command, stdout=iostat_output_file)
    monitor_processes.append(iostat_process)
    monitor_output_files.append(iostat_output_file)

    if vnstat_interface != None:
        vnstat_output_file = open(
            os.path.join(log_directory, "vnstat-%s.out" % (hostname)), "w")
        command = ["bash", "-c", "while [ 0 ]; do vnstat -i %s -tr; done" % (
                vnstat_interface)]
        vnstat_process = subprocess.Popen(command, stdout=vnstat_output_file)
        monitor_processes.append(vnstat_process)
        monitor_output_files.append(vnstat_output_file)

    return (monitor_processes, monitor_output_files)

def stop_monitors(monitor_processes, monitor_output_files):
    for process in monitor_processes:
        process.terminate()
    for output_file in monitor_output_files:
        output_file.flush()
        output_file.close()
