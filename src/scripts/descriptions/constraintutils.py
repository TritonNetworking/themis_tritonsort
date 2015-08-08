import json
import consts
import os,sys

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             os.pardir))

from common.unitconversion import convert

def load_constraints(filename):
    with open(filename, 'r') as fp:
        constraints = json.load(fp)

    for phase in constraints:
        for stage in constraints[phase][consts.STAGES_KEY]:
            stage_info = constraints[phase][consts.STAGES_KEY][stage]

            if consts.IO_BOUND_KEY in stage_info:
                (rate, sep, unit) = stage_info[
                    consts.MAX_RATE_PER_WORKER_KEY].partition(' ')
                rate = float(rate)

                rate_Bps = convert(rate, unit, "Bps")

                stage_info[consts.MAX_RATE_PER_WORKER_KEY] = rate_Bps

    return constraints

def get_io_bound_stages(constraints):
    """
    For each phase, returns a list of I/O bound stages
    """
    io_bound_stages = {}

    for phase in constraints:
        io_bound_stages[phase] = []
        for stage in constraints[phase][consts.STAGES_KEY]:
            stage_info = constraints[phase][consts.STAGES_KEY][stage]

            if consts.IO_BOUND_KEY in stage_info:
                max_stages = stage_info[consts.MAX_WORKERS_KEY]
                rate_per_stage = stage_info[consts.MAX_RATE_PER_WORKER_KEY]

                io_bound_stages[phase].append(
                    (stage, rate_per_stage * max_stages))

    return io_bound_stages
