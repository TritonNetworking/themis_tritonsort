import os, threading, abc, time

class TimedIterateThread(threading.Thread):
    __metaclass__ = abc.ABCMeta

    def __init__(self, iter_sleep = None):
        super(TimedIterateThread, self).__init__()

        self.exit = threading.Event()
        self.iter_sleep = iter_sleep

    @abc.abstractmethod
    def iterate(self):
        pass

    @abc.abstractmethod
    def cleanup(self):
        pass

    def run(self):
        while not self.exit.is_set():
            self.iterate()

            if self.iter_sleep is not None:
                time.sleep(self.iter_sleep)

        self.cleanup()

    def shutdown(self):
        self.exit.set()
        self.join()

def create_log_directory(log_directory):
    # Log directory might refer to ~
    log_directory = os.path.expanduser(log_directory)
    if not os.path.exists(log_directory):
        print "Creating %s" % log_directory
        os.makedirs(log_directory)

    for sub_directory in ["node_coordinators", "run_logs"]:
        directory = os.path.join(log_directory, sub_directory)
        if not os.path.exists(directory):
            print "Creating %s" % directory
            os.makedirs(directory)

    return log_directory

def create_batch_directory(log_directory, batch_id):
    batch_directory = os.path.join(
        log_directory, "run_logs", "batch_%d" % batch_id)
    if not os.path.exists(batch_directory):
        os.makedirs(batch_directory)
    return batch_directory
