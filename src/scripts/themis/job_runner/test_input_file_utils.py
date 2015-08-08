#!/usr/bin/env python

import unittest, itertools, os, sys
from input_file_utils import generate_read_requests

sys.path.append(os.path.join(os.path.dirname(__file__), os.pardir))

import utils

class TestGenerateReadRequests(unittest.TestCase):
    def test_single_job(self):
        worker_inputs = {
            "host_A" : {
                0 : [("file_A_1", 1000), ("file_A_2", 3000)],
                1 : [("file_A_3", 1000)],
                2 : [("file_A_4", 500), ("file_A_5", 6000)],
                3 : [("file_A_6", 1000), ("file_A_7", 2000)]
                },
            "host_B" : {
                0 : [("file_A_2", 3000)],
                1 : [("file_A_3", 1000)],
                2 : [("file_A_4", 500)],
                3 : [("file_A_6", 1000), ("file_A_7", 2000)]
                }
            }

        phase_zero_prefix_size = 4242
        job_ids = [1]

        read_requests = generate_read_requests(
            [worker_inputs], phase_zero_prefix_size, job_ids)

        for host, worker in utils.flattened_keys(worker_inputs):
            worker_reqs = read_requests[host][worker]

            self.assertEqual(
                2 * (len(worker_inputs[host][worker]) + 1), len(worker_reqs))

            req_index = 0

            # Should first have a phase zero prefix for each file
            for filename, length in worker_inputs[host][worker]:
                req = worker_reqs[req_index]

                self.assertEqual(job_ids, req["job_ids"])
                self.assertEqual(filename, req["path"])
                self.assertEqual(0, req["offset"])
                self.assertEqual(0, req["type"])
                self.assertEqual(phase_zero_prefix_size, req["length"])

                req_index += 1

            # Halt request for phase zero should come after that
            self.assertEqual(1, worker_reqs[req_index]["type"])
            self.assertEqual(job_ids, worker_reqs[req_index]["job_ids"])

            req_index += 1

            # Next, should have a full phase one read request for each file
            for filename, length in worker_inputs[host][worker]:
                req = worker_reqs[req_index]

                self.assertEqual(job_ids, req["job_ids"])
                self.assertEqual(filename, req["path"])
                self.assertEqual(0, req["offset"])
                self.assertEqual(0, req["type"])
                self.assertEqual(length, req["length"])

                req_index += 1

            # These read requests should be followed by a halt request
            self.assertEqual(1, worker_reqs[req_index]["type"])
            self.assertEqual(job_ids, worker_reqs[req_index]["job_ids"])

            req_index += 1

    def test_multi_job_scan_share(self):
        job_ids = [1, 2]
        phase_zero_prefix_size = 4242

        worker_inputs = []
        worker_inputs.append({
                "host_A" : {
                    0 : [("file_A_1", 1000), ("file_A_2", 3000)],
                    1 : [("file_A_3", 1000)],
                    2 : [("file_A_4", 500), ("file_A_5", 6000)],
                    3 : [("file_A_6", 1000), ("file_A_7", 2000),
                         ("file_A_8", 1000)]
                    },
                "host_B" : {
                    0 : [("file_A_2", 3000)],
                    1 : [("file_A_3", 1000)],
                    2 : [("file_A_4", 500)],
                    3 : [("file_A_6", 1000)]
                    }
                })
        worker_inputs.append({
                "host_A" : {
                    0 : [("file_A_1", 1000), ("file_A_2", 3000)],
                    1 : [("file_A_3", 1000)],
                    2 : [("file_A_4", 500), ("file_A_5", 6000)],
                    3 : [("file_A_6", 1000), ("file_A_7", 2000)]
                    },
                "host_B" : {
                    0 : [("file_A_2", 3000)],
                    1 : [("file_A_3", 1000)],
                    2 : [("file_A_4", 500)],
                    3 : [("file_A_7", 2000)]
                    }
                })

        read_requests = generate_read_requests(
            worker_inputs, phase_zero_prefix_size, job_ids)

        # Expected file assignments after scan-sharing merge

        expected_assignments = {
            "host_A" : {
                0 : [("file_A_1", 1000, [1,2]), ("file_A_2", 3000, [1,2])],
                1 : [("file_A_3", 1000, [1,2])],
                2 : [("file_A_4", 500, [1,2]), ("file_A_5", 6000, [1,2])],
                3 : [("file_A_6", 1000, [1,2]), ("file_A_7", 2000, [1,2]),
                     ("file_A_8", 1000, [1])]
                },
            "host_B" : {
                0 : [("file_A_2", 3000, [1,2])],
                1 : [("file_A_3", 1000, [1,2])],
                2 : [("file_A_4", 500, [1,2])],
                3 : [("file_A_6", 1000, [1]), ("file_A_7", 2000, [2])]
                }
            }

        for host, worker in utils.flattened_keys(expected_assignments):
            assignments = expected_assignments[host][worker]
            reqs = read_requests[host][worker]

            self.assertEqual((len(assignments) + 1) * 3, len(reqs))

            req_index = 0

            # Should first have a phase zero prefix for each file
            for job_id in job_ids:
                for assignment in assignments:
                    read_request = reqs[req_index]

                    self.assertEqual(assignment[0], read_request["path"])
                    self.assertEqual(
                        phase_zero_prefix_size, read_request["length"])
                    self.assertEqual([job_id], read_request["job_ids"])
                    self.assertEqual(0, read_request["offset"])
                    self.assertEqual(0, read_request["type"])

                    req_index += 1

                # These read requests should be followed by a halt request
                self.assertEqual(1, reqs[req_index]["type"])
                self.assertEqual([job_id], reqs[req_index]["job_ids"])
                req_index += 1

            # Next, should have a full phase one read request for each file
            for assignment in assignments:
                read_request = reqs[req_index]

                self.assertEqual(assignment[0], read_request["path"])
                self.assertEqual(assignment[1], read_request["length"])
                self.assertEqual(assignment[2], read_request["job_ids"])
                self.assertEqual(0, read_request["offset"])
                self.assertEqual(0, read_request["type"])

                req_index += 1

            # These read requests should be followed by a halt request
            self.assertEqual(1, reqs[req_index]["type"])
            self.assertEqual(job_ids, reqs[req_index]["job_ids"])

if __name__ == "__main__":
    unittest.main()
