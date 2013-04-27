# -*- coding: utf-8 -*-
from __future__ import print_function

import logging

from collections import defaultdict, deque

from system import BusinessObject
from server import SystemClient

logger = logging.getLogger('spf')


class RoutingState(object):
    def __init__(self):
        self.servers = set()
        self.neighbor_lists = defaultdict(set)

    def handle_disconnect(self, routing_id):
        if routing_id in self.neighbor_lists:
            for neighbor in self.neighbor_lists[routing_id]:
                self.neighbor_lists[neighbor].remove(routing_id)
            del self.neighbor_lists[routing_id]
        else:
            for node, neighbors in self.neighbor_lists.iteritems():
                if routing_id not in neighbors:
                    continue
                self.neighbor_lists[node].remove(routing_id)

        logger.info(u"Handled disconnect {0}".format(routing_id))

    def handle_subscription(self, server_routing_id, client_routing_id):
        self.servers.add(server_routing_id)
        self.neighbor_lists[server_routing_id].add(client_routing_id)
        self.neighbor_lists[client_routing_id].add(server_routing_id)

        logger.info(self.neighbor_lists[server_routing_id])
        logger.info(self.neighbor_lists[client_routing_id])

        logger.info(u"Handled subscription from {0}".format(client_routing_id))

    def handle_announcement(self, routing_id, routing_ids):
        self.servers.add(routing_id)
        if routing_id in self.neighbor_lists:
            for neighbor in self.neighbor_lists[routing_id]:
                self.neighbor_lists[neighbor].remove(routing_id)
            del self.neighbor_lists[routing_id]

        for neighbor in routing_ids:
            self.neighbor_lists[neighbor].add(routing_id)
            self.neighbor_lists[routing_id].add(neighbor)

        logger.debug(u"Handled announcement from {0}".format(routing_id))

    def bmgraph(self):
        lines = set()
        for node, neighbors in self.neighbor_lists.iteritems():
            for neighbor in neighbors:
                n1, n2 = sorted([node, neighbor])
                if n1 in self.servers:
                    n1 = u"Server_{0}".format(n1)
                else:
                    n1 = u"Client_{0}".format(n1)
                if n2 in self.servers:
                    n2 = u"Server_{0}".format(n2)
                else:
                    n2 = u"Client_{0}".format(n2)

                lines.add(u"{0} {1} is_neighbor_of".format(n1, n2))

        return '\n'.join(list(lines)) + '\n'

    def nodes(self):
        return self.neighbor_lists.keys()

    def shortest_path(self, source, destination, visited=None):
        assert(source in self.neighbor_lists)

        if destination not in self.neighbor_lists:
            return None, None

        if visited is None:
            visited = set()
        visited = visited.copy()
        visited.add(source)

        paths = []
        for neighbor in self.neighbor_lists[source]:
            if neighbor == destination:
                return 0, [neighbor]

            if neighbor in visited:
                continue

            length, path = self.shortest_path(neighbor, destination,
                                              visited=visited)
            if (length, path) != (None, None):
                paths.append((length, path))

        paths = sorted(paths, key=lambda t: t[0])

        if len(paths) == 0:
            return None, None

        return paths[0][0] + 1, paths[0][1]
