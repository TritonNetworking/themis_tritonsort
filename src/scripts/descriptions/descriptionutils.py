import os,sys,json
from networkx import DiGraph,  NetworkXError, NetworkXUnfeasible, to_agraph
from networkx.algorithms.dag import topological_sort
import validateDescription
import consts
import re
import copy

class Description(object):
    def __init__(self, description):
        if not os.path.exists(description):
            print >>sys.stderr, "Can't find description directory '%s'" % \
                (description)
            sys.exit(1)

        validator = validateDescription.DescriptionValidator()

        validator.validateDescription(description)

        # If this didn't throw, description is valid
        self.description = description

        self.structureInfo = self._getFileJsonContents(
            consts.STRUCTURE_FILENAME)
        self.stageInfo = self._getFileJsonContents(consts.STAGE_INFO_FILENAME)

    def _getFileHandle(self, filename):
        totalPath = os.path.join(self.description, filename)
        if not os.path.exists(totalPath):
            print >>sys.stderr, "Can't find file '%s'" % (totalPath)

        fp = open(totalPath, 'r')
        return fp


    @staticmethod
    def _loadJsonFile(fp):
        try:
            jsonObj = json.load(fp)
        except ValueError as e:
            print >>sys.stderr, "Parsing JSON file '%s' failed: %s" % \
                (filename, e)
            sys.exit(1)
        finally:
            fp.close()

        return jsonObj

    def _getFileJsonContents(self, filename):
        fp = self._getFileHandle(filename)
        jsonObj = Description._loadJsonFile(fp)
        return jsonObj

    def getPhaseList(self):
        return self.structureInfo[consts.PHASES_KEY]

    def getStageGraphs(self):
        graphs = {}

        for phase in self.structureInfo[consts.PHASES_KEY]:
            graphs[phase] = self.getStageGraph(phase)

        return graphs

    def getStageGraph(self, phase):
        graph = DiGraph()

        for stage in self.structureInfo[phase][consts.STAGES_KEY].keys():
            graph.add_node(stage)
            graph.node[stage]["name"] = str(stage)

        for edge in self.structureInfo[phase][consts.STAGE_TO_STAGE_KEY]:
            graph.add_edge(edge[consts.SRC_KEY], edge[consts.DEST_KEY])

        return graph

    def getClusterGraph(self, host_list, phase):
        return self._generateClusterGraph(host_list, phase, self.getStageGraph)

    def _generateClusterGraph(self, host_list, phase, new_host_graph_function):
        cluster_graph = DiGraph()

        for host_id, host in enumerate(host_list):
            host_graph = new_host_graph_function(phase)

            for node in host_graph.nodes():
                stage_info = self.structureInfo[phase][consts.STAGES_KEY][node]
                if ((consts.ACTIVE_NODES_KEY in stage_info and
                    host_id in stage_info[consts.ACTIVE_NODES_KEY]) or
                    (consts.ACTIVE_NODES_KEY not in stage_info)):

                    new_node_name = self._getHostNodeName(host, node)
                    cluster_graph.add_node(new_node_name)

                    for key, value in host_graph.node[node].items():
                        cluster_graph.node[new_node_name][copy.deepcopy(key)] =\
                            copy.deepcopy(value)

            for edge in host_graph.edges():
                source_node_name = self._getHostNodeName(host, edge[0])
                dest_node_name = self._getHostNodeName(host, edge[1])

                if source_node_name in cluster_graph and dest_node_name \
                        in cluster_graph:
                    cluster_graph.add_edge(source_node_name, dest_node_name)

        if consts.NETWORK_CONN_KEY not in self.structureInfo[phase]:
            # If there are no network connections, the hosts' graphs are
            # disjoint
            return cluster_graph

        net_connections = self.structureInfo[phase][consts.NETWORK_CONN_KEY]

        for net_connection in net_connections:
            connection_type = net_connection[consts.CONN_TYPE_KEY]
            src_stage = net_connection[consts.SRC_KEY]
            dest_stage = net_connection[consts.DEST_KEY]

            if consts.CONN_BYPASS_LOCALHOST in net_connection:
                localhost_bypass = net_connection[consts.CONN_BYPASS_LOCALHOST]
            else:
                localhost_bypass = False

            src_hosts = []
            dest_hosts = []

            if connection_type == consts.CONN_TYPE_ONE_TO_MANY:
                src_node = net_connection[consts.CONN_SRC_NODE]
                src_host = host_list[src_node]
                src_hosts.append(src_host)

                for dest_host in host_list:
                    dest_hosts.append(dest_host)

            elif connection_type == consts.CONN_TYPE_MANY_TO_ONE:
                dest_node = net_connection[consts.CONN_DEST_NODE]
                dest_host = host_list[dest_node]
                dest_hosts.append(dest_host)

                for src_host in host_list:
                    src_hosts.append(src_host)

            elif connection_type == consts.CONN_TYPE_MANY_TO_MANY:
                for src_host in host_list:
                    src_hosts.append(src_host)

                for dest_host in host_list:
                    dest_hosts.append(dest_host)

            for src_host in src_hosts:
                for dest_host in dest_hosts:
                    if localhost_bypass and src_host == dest_host:
                        continue

                    src_vertex = self._getHostNodeName(src_host, src_stage)
                    dest_vertex = self._getHostNodeName(dest_host, dest_stage)
                    if (src_vertex in cluster_graph and
                        dest_vertex in cluster_graph):

                        cluster_graph.add_edge(src_vertex, dest_vertex)

        return cluster_graph

    def drawClusterGraph(self, host_list, phase, filename):
        cluster_graph = self.getClusterGraph(host_list, phase)
        self._drawGraph(cluster_graph, filename)

    def _getHostNodeName(self, host, node):
        return (host, node)

    def getStageNames(self, phase):
        return self.structureInfo[phase][consts.STAGES_KEY].keys()

    def getStageOrderings(self):
        stageLists = {}
        for phase in self.structureInfo[consts.PHASES_KEY]:
            stageLists[phase] = self.getStageOrdering(phase)

        return stageLists

    def getStageOrdering(self, phase):
        graph = self.getStageGraph(phase)

        try:
            stageList = topological_sort(graph)
        except NetworkXError as e:
            print >>sys.stderr, "getStageOrdering() failed: stage graph "\
                "is not directed"
            sys.exit(1)
        except NetworkXUnfeasible as e:
            print >>sys.stderr, "getStageOrdering(): stage graph contains "
            "cycles; falling back to listing stages in the order they are "
            "specified"
            stageList = self.structureInfo[phase][consts.STAGES_KEY].keys()

        return stageList

    def getWorkerCountRegex(self, phase, stage):
        workerCountParam = self.structureInfo[phase][consts.STAGES_KEY][stage]\
            [consts.COUNT_PARAM_KEY]
        return re.compile("\[PARAM\]\s+%s\s+=\s+(\d+)" % (workerCountParam))

    def _drawGraph(self, graph, filename):
        graphviz_graph = to_agraph(graph)

        graphviz_graph.draw(filename, prog="dot")
