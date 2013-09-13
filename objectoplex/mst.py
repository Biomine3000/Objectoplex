# -*- coding: utf-8 -*-
from __future__ import print_function

from collections import deque

def collect_all_nodes(graph):
    result = set()

    for a, b in graph.iterkeys():
        result.add(a)
        result.add(b)

    return result

def edges_sorted_by_weight(graph):
    """
    Returns edges of the graph sorted by weight, descending.
    """
    return [edge
            for edge, weight in sorted(graph.items(),
                                       key=lambda x: x[1], reverse=True)]

def best_edge(graph, current, nodes):
    """
    Chooses the best edge of the edges of the given node.

    Takes a graph, current node and a set of nodes as parameters.

    Typically one would pass in the already-connected nodes of a minimum
    spanning tree as the nodes-parameter.
    """
    candidates = set()

    for edge, weight in graph.iteritems():
        a, b = edge
        for node in nodes:
            if a == current and b not in nodes:
                candidates.add((edge, weight))
            elif b == current and a not in nodes:
                candidates.add((edge, weight))
            elif current in (a, b) and current not in nodes:
                candidates.add((edge, weight))

    if len(candidates) == 0:
        return None

    max = sorted(candidates, key=lambda x: x[1], reverse=True)[0]
    return max[0]

def is_single_component(graph):
    all_nodes = collect_all_nodes(graph)

    visited = set()
    queue = deque([list(all_nodes)[0]])
    while len(queue) > 0:
        current = queue.popleft()
        for edge in graph.iterkeys():
            a, b = edge
            if current == a and b not in visited:
                queue.append(b)
                visited.add(a)
            elif current == b and a not in visited:
                queue.append(a)
                visited.add(b)
        visited.add(current)

    if len(visited) == len(all_nodes):
        return True

    return False


def minimum_spanning_tree(graph):
    """
    Graph is a dict where the keys are alphabetically ordered node names, e.g.
    a graph with connected nodes A and B with an edge weight of 1.0 would be
    represented by the dict { (A, B): 1.0 }.

    This function returns the graph passed in as a minimum spanning tree, with
    all non-minimum edges removed, but still spanning all the same nodes as
    the original graph.
    """
    all_nodes = collect_all_nodes(graph)
    reverse_sorted_edges = sorted(edges_sorted_by_weight(graph), reverse=True)

    sorted_nodes = sorted(list(all_nodes))

    # Start from alphabetically first to be deterministic
    current = sorted_nodes[0]

    tree_nodes = set([current])
    tree_edges = set()

    iterations = 0
    while len(tree_nodes) < len(all_nodes):
        if iterations > len(graph.keys()) + 2:
            raise TooManyIterations()
        
        if current is None:
            current = sorted(all_nodes - tree_nodes)[0]

        while True:
            iterations += 1
            edge = best_edge(graph, current, tree_nodes)

            if edge is None:
                current = None
                break

            tree_edges.add(edge)

            a, b = edge

            tree_nodes.add(a)
            tree_nodes.add(b)
            if current == a:
                current = b
            elif current == b:
                current = a
            else:
                raise Exception("Invalid case")

    # print(iterations)
    return tree_edges
