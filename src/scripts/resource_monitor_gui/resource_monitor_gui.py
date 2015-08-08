#!/usr/bin/env python

import os
import sys
import redis
import argparse
import jinja2
import json
import socket
import bottle
import math
import collections
import StringIO
import base64
import urllib
import networkx as nx
import matplotlib.pyplot as plt

sys.path.append(os.path.join(os.path.abspath(os.path.dirname(__file__)),
                             os.pardir))

from common.unitconversion import convert
from themis import utils as themis_utils

import constants

class InfoTable(object):
    def __init__(self):
        self.table_vals = {}
        self.cell_styles = {}
        self.ordered_table_rows = []
        self.ordered_table_columns = []
        self.totals = {}
        self.cell_in_bytes = {}

    def set_row_ordering(self, ordering):
        filtered_ordering = filter(lambda x: x in self.ordered_table_rows,
                                   ordering)
        self.ordered_table_rows = filtered_ordering

    def add(self, row, column, value, style=None, in_bytes=False):
        if row not in self.table_vals:
            self.table_vals[row] = {}
            self.cell_styles[row] = {}
            if type(value) in [int, float]:
                self.totals[row] = 0
            else:
                self.totals[row] = None

            self.ordered_table_rows.append(row)
            self.ordered_table_rows.sort()

        if column not in self.ordered_table_columns:
            self.ordered_table_columns.append(column)
            self.ordered_table_columns.sort()

        self.table_vals[row][column] = value

        if self.totals[row] != None and type(value) in [int, float]:
            self.totals[row] += value

        self.cell_styles[row][column] = style

        if row not in self.cell_in_bytes:
            self.cell_in_bytes[row] = {}
        self.cell_in_bytes[row][column] = (in_bytes == True)

    def get(self, row, column):
        try:
            cell = self.table_vals[row][column]
        except KeyError, e:
            cell = "N/A"
        finally:
            return cell

    def make_bytes_human_readable(self):
        for row in self.table_vals:
            for col in self.table_vals[row]:
                if row.find("byte") != -1 or self.cell_in_bytes[row][col]:
                    self.table_vals[row][col] = self._make_cell_readable(
                        self.table_vals[row][col])

        for row in self.totals:
            if row.find("byte") != -1:
                self.totals[row] = self._make_cell_readable(self.totals[row])

    def _make_cell_readable(self, cell_value):
        # Make sure we don't try to convert non-numeric types
        if type(cell_value) not in [int, float]:
            return cell_value

        units = ["B", "KiB", "MiB", "GiB", "TiB"]

        if cell_value > 0:
            # floor(log_2(X)) gives the closest integer power of 2 to
            # X. Dividing it by 10 gives the unit (2^0 = bytes, 2^10 =
            # kibibytes, and so on, so 0 = bytes, 1 = kibibytes, etc.)
            cell_value_dest_unit = units[
                min(int(math.floor(math.log(cell_value, 2) / 10)),
                    len(units) - 1)]

            cell_value = "%.2f %s" % (
                convert(cell_value, "B", cell_value_dest_unit),
                cell_value_dest_unit)
        else:
            cell_value = "0 B"

        return cell_value


class ResourceMonitorGUI(object):
    def __init__(self, hosts, port, debug):
        self.hosts = hosts
        self.port = port
        self.debug = debug
        self.env = jinja2.Environment(
            loader = jinja2.FileSystemLoader(
                os.path.join(os.path.dirname(__file__), 'templates')),
            trim_blocks = True)

    def format_pool_template(self, struct_key, struct, pools):
        pool_id = struct["id"]

        pool_name_str = "%s - %d" % (struct_key, pool_id)

        percent_available = (float(struct["availability"]) /
                             float(struct["capacity"]))

        color_intensity = percent_available * 255

        cell_color = "#ff%02x%02x" % (color_intensity, color_intensity)

        pools.add(pool_name_str, "% Buffers Available",
                  "%.2f%%" % (percent_available * 100),
                  'style="background-color: %s"' % (cell_color))

        pools.add(pool_name_str, "Buffers Available",
                  struct["availability"])
        pools.add(pool_name_str, "Capacity", struct["capacity"])

    def format_worker_template(self, struct_key, struct, workers):
        reserved_keys = ["id", "type"]

        worker_id = struct["id"]

        for (key,value) in sorted(struct.items()):
            if key in reserved_keys:
                continue

            if struct_key not in workers:
                workers[struct_key] = InfoTable()

            workers[struct_key].add(key, worker_id, value)

    def format_stage_template(self, struct_key, struct, stages, workers,
                              stage_graph):
        completed_prev_trackers = struct["completed_prev_trackers"]
        prev_trackers = struct["prev_trackers"]
        spawned = struct["spawned"]
        teardown_initiated = struct["teardown_initiated"]
        teardown_complete = struct["teardown_complete"]
        worker_queue_sizes = struct["worker_queue_sizes"]

        for worker_id, queue_size in enumerate(worker_queue_sizes):
            if struct_key not in workers:
                workers[struct_key] = InfoTable()

            workers[struct_key].add("queue_size", worker_id, queue_size)

        if "next_stages" in struct:
            for next_stage in struct["next_stages"]:
                stage_graph.add_edge(struct_key, next_stage)

        current_state = "Unknown"
        style = None

        waiting_to_spawn = not spawned
        running = spawned and not teardown_initiated
        tearing_down = spawned and teardown_initiated and \
            not teardown_complete
        completed = spawned and teardown_initiated and teardown_complete

        if waiting_to_spawn:
            current_state = "Waiting to Spawn"
            style = 'class="gray"'
        elif running:
            current_state = "Running"
            style = 'class="green"'
        elif tearing_down:
            current_state = "Finishing Existing Work/Tearing Down"
            style = 'class="yellow"'
        elif completed:
            current_state = "Completed"
            style = 'class="red"'
        else:
            current_stats = "Unknown"
            style = 'class="orange"'

        stages.add(struct_key, "Current State", current_state, style)

    def format_memory_usage_template(self, client_struct, memory_allocations,
                                     workers):
        worker_memory_usage = client_struct["worker_memory_usage"]

        stage_memory_usage_totals = collections.defaultdict(int)
        total_memory_used = 0

        for memory_usage_struct in worker_memory_usage:
            stage = memory_usage_struct["stage"]
            memory_used = memory_usage_struct["memory_used"]
            worker_id = memory_usage_struct["id"]

            total_memory_used += memory_used

            stage_memory_usage_totals[stage] += memory_used
            if stage not in workers:
                workers[stage] = InfoTable()

            workers[stage].add("memory_used_bytes", worker_id, memory_used)

        for stage, usage_total in stage_memory_usage_totals.items():
            memory_allocations.add(stage, "Memory Used", usage_total,
                                   in_bytes = True)

            if total_memory_used > 0:
                percent_total = usage_total * 100.0 / float(total_memory_used)
            else:
                percent_total = 0.0

            memory_allocations.add(stage, "% Total", "%.2f%%" % (percent_total))

    def format_mmap_bytes_template(self, client_struct, mmap_bytes):
        mmap_byte_totals = client_struct["mmap_byte_totals"]

        for mmap_disk in mmap_byte_totals:
            disk = mmap_disk["disk"]
            byte_total = mmap_disk["bytes"]

            mmap_bytes.add(disk, "Memory Mapped Bytes", byte_total,
                           in_bytes = True)

    def format_index_template(self, hostname, struct):
        workers = {}
        stages = InfoTable()
        memory_allocations = InfoTable()
        mmap_bytes = InfoTable()
        misc = {}
        stage_graph = nx.DiGraph()

        for client_key, client_struct_list in struct.items():
            for client_struct in client_struct_list:
                self.format_client_struct(
                    client_key, client_struct, workers, stages,
                    memory_allocations, mmap_bytes, stage_graph, misc)

        stages.make_bytes_human_readable()

        for table in workers.values():
            table.make_bytes_human_readable()

        for table in misc.values():
            table.make_bytes_human_readable()

        memory_allocations.make_bytes_human_readable()
        mmap_bytes.make_bytes_human_readable()

        topological_order = []
        components = nx.algorithms.components.weakly_connected_components(
            stage_graph)

        # Topologically sort each component
        toposorted_components = []
        topo_levels = {}

        for component in components:
            # Topologically sort the component
            component_toposorted = nx.topological_sort(stage_graph, component)

            # Get each node's "level" in the topological sort order
            component_topo_levels = self.label_toposort_levels(
                stage_graph, component_toposorted)

            for key, value in component_topo_levels.items():
                topo_levels[key] = value

            topological_order.extend(component_toposorted)
            toposorted_components.append(component)

        node_positions = self.get_node_positions(
            stage_graph, toposorted_components, topo_levels)

        nx.draw(stage_graph, pos=node_positions, node_color="cyan",
                node_size=1000)

        img_data = StringIO.StringIO()
        plt.savefig(img_data, format='png')
        img_data.seek(0)
        plt.clf()

        stages.set_row_ordering(topological_order)
        memory_allocations.set_row_ordering(topological_order)

        def worker_sort(worker):
            for i, stage_name in enumerate(topological_order):
                if worker.find(stage_name) == 0:
                    return i
            return -1

        worker_order = sorted(workers.keys(), key=worker_sort)

        index_template_values = {
            "memory_allocations" : memory_allocations,
            "mmap_bytes" : mmap_bytes,
            "workers" : workers,
            "stages" : stages,
            "misc" : misc,
            "hostname" : hostname,
            "port" : self.port,
            "worker_order" : worker_order,
            "graph_image_data" : ("data:image/png;base64," +
                                  urllib.quote(base64.b64encode(img_data.buf)))
            }
        return index_template_values

    def label_toposort_levels(self, stage_graph, component):
        levels = {}

        def label_node(stage_graph, stage):
            if "toposort_level" in stage_graph[stage]:
                return

            in_edges = stage_graph.in_edges(stage)

            if len(in_edges) == 0:
                levels[stage] = 0
            else:
                for edge in in_edges:
                    label_node(stage_graph, edge[0])

                levels[stage] = max(
                    (levels[in_stage] for (in_stage, my_stage) in in_edges)) + 1

        for stage in component:
            label_node(stage_graph, stage)

        return levels

    def get_node_positions(self, stage_graph, components, topo_levels):
        node_positions = {}

        y_coordinate = 0

        for component_id, component in enumerate(components):
            (component_positions, width) = self.get_component_positions(
                stage_graph, component, 0, y_coordinate, topo_levels)

            y_coordinate += width

            for (key, value) in component_positions.items():
                node_positions[key] = value

        return node_positions

    def get_component_positions(self, stage_graph, component, x_start, y_start,
                                topo_levels):
        positions = {}

        # Subdivide nodes by topological sort level
        level_stages = collections.defaultdict(list)

        for stage in component:
            level_stages[topo_levels[stage]].append(stage)

        width = max((len(x) for x in level_stages.values()))

        y_coordinates = {}

        for level in sorted(level_stages.keys()):
            stages = level_stages[level]

            for i, stage in enumerate(stages):
                in_edges = stage_graph.in_edges(stage)

                if stage not in y_coordinates:
                    if len(in_edges) == 0:
                        y_coordinates[stage] = i
                    else:
                        y_coordinates[stage] = max(
                            (y_coordinates[in_stage]
                             for (in_stage, my_stage) in in_edges))

                positions[stage] = (y_coordinates[stage] + y_start, -level)

        return (positions, width)

    def format_client_struct(self, client_key, client_struct, workers, stages,
                             memory_allocations, mmap_bytes, stage_graph, misc):
        if client_key == "memory_allocator":
            stage_memory_allocations = self.format_memory_usage_template(
                client_struct, memory_allocations, workers)
        elif client_key == "memory_mapped_file_deadlock_resolver":
            self.format_mmap_bytes_template(client_struct, mmap_bytes)
        elif "type" in client_struct:
            struct_type = client_struct["type"]

            if struct_type == "worker":
                self.format_worker_template(client_key, client_struct, workers)
            elif struct_type == "tracker":
                self.format_stage_template(client_key, client_struct, stages,
                                           workers, stage_graph)
        else:
            if client_key not in misc:
                misc[client_key] = InfoTable()

            for (key, val) in client_struct.items():
                misc[client_key].add(key, "Value", val)

    def index(self):
        template = self.env.get_template('host_list.template')

        return template.render(hosts = self.hosts).strip()

    def node(self, hostname):
        if self.debug:
            fp = open("test.json")
            server_output = fp.read()
        else:
            try:
                sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                sock.connect((hostname, self.port))

                server_output = ""
                while True:
                    data = sock.recv(1024)

                    if data == "":
                        break
                    else:
                        server_output += data
                sock.close()

            except socket.error, e:
                template = self.env.get_template('connection_problems.template')
                rendered_template = template.render(hostname = hostname,
                                                   error = str(e))
                return rendered_template.strip()

        struct = json.loads(server_output)

        template_input = self.format_index_template(hostname, struct)

        template = self.env.get_template('resource_monitor_main.template')
        rendered_template = template.render(**template_input)

        return rendered_template.strip()

def main():
    parser = argparse.ArgumentParser(description="A web server that visualizes "
                                     "the Themis ResourceMonitor data")
    parser.add_argument("resource_monitor_port", help="Port that "
                        "the ResourceMonitor listens for connections on ("
                        "defined by the MONITOR_PORT application config "
                        "parameter)")
    parser.add_argument("-d", "--debug", help="enables debug mode",
                            default=False, action="store_true")
    parser.add_argument("-l", "--localaddress", default="0.0.0.0",
                        help="HTTP local address to listen on "
                        "(default: %(default)s)")
    parser.add_argument("-p", "--port",
                        help="HTTP port to listen on (default: %(default)s)",
                        type=int, default=8080)
    themis_utils.add_redis_params(parser)

    args = parser.parse_args()

    redis_client = redis.StrictRedis(host=args.redis_host, port=args.redis_port,
                                     db=args.redis_db)
    hosts = sorted(redis_client.smembers("nodes"))

    rmgui = ResourceMonitorGUI(hosts = hosts,
                               port=int(args.resource_monitor_port),
                               debug=args.debug)

    bottle.route("/")(rmgui.index)
    bottle.route("/node/<hostname>")(rmgui.node)

    try:
        bottle.run(host=args.localaddress, port=args.port)
    except socket.error, e:
        print e
        # Indicate that we can't bind, so that scripts calling this one can
        # handle that case specially
        return constants.CANNOT_BIND

if __name__ == "__main__":
    sys.exit(main())
