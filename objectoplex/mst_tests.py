#!/usr/bin/env python
# -*- coding: utf-8 -*-
from __future__ import print_function

import string
from random import choice, random as random

from unittest import TestCase
from unittest import main as unittest_main

from mst import collect_all_nodes, edges_sorted_by_weight, best_edge
from mst import minimum_spanning_tree, is_single_component

def synthetic_graph(node_count):
    node_id = lambda: ''.join(choice(string.ascii_uppercase + string.digits)
                              for x in range(6))
        
    nodes = set([])
    for i in xrange(node_count):
        nodes.add(node_id())

    graph = {}

    edge_count = len(nodes) * 2
    while edge_count > 0:
        a = choice(list(nodes))
        b = choice(list(nodes))

        a, b = sorted([a, b])

        if a == b or (a, b) in graph:
            continue

        graph[(a, b)] = random()
        edge_count -= 1

    return graph

class CollectAllNodesTestCase(TestCase):
    def test_collect_all_nodes(self):
        graph = { ('A', 'B'): 1.0,
                  ('B', 'C'): 2.5 }

        all_nodes = collect_all_nodes(graph)
        self.assertTrue('A' in all_nodes)
        self.assertTrue('B' in all_nodes)
        self.assertTrue('C' in all_nodes)

class EdgesSortedByWeightTestCase(TestCase):
    def test_edges_sorted_by_weight(self):
        graph = { ('A', 'B'): 0.1,
                  ('A', 'D'): 0.4,
                  ('B', 'C'): 0.2 }

        sorted_edges = edges_sorted_by_weight(graph)

        self.assertEqual(sorted_edges[0], ('A', 'D'))
        self.assertEqual(sorted_edges[-1], ('A', 'B'))

class BestEdgeTestCase(TestCase):
    def test_best_edge(self):
        graph = { ('A', 'B'): 0.1,
                  ('A', 'D'): 0.4,
                  ('B', 'C'): 0.2,
                  ('A', 'C'): 0.8 }
        
        self.assertEqual(('A', 'C'), best_edge(graph, 'A', set(['A'])))
        self.assertEqual(('A', 'D'), best_edge(graph, 'A', set(['A', 'C'])))
        self.assertEqual(('A', 'B'), best_edge(graph, 'A', set(['A', 'C', 'D'])))

class MinimumSpanningTreeTestCase(TestCase):
    def test_minimum_spanning_tree_simple_case(self):
        graph = { ('A', 'B'): 0.1,
                  ('A', 'D'): 0.4,
                  ('B', 'C'): 0.2,
                  ('A', 'C'): 0.8 }
        
        self.assertEqual(set([('A', 'C'), ('A', 'D'), ('B', 'C')]),
                         minimum_spanning_tree(graph))

    def test_minimum_spanning_tree_less_simple_case(self):
        graph = { ('A', 'B'): 0.1,
                  ('A', 'D'): 0.4,
                  ('B', 'C'): 0.2,
                  ('A', 'C'): 0.8,
                  ('D', 'E'): 0.2,
                  ('E', 'F'): 0.2,
                  ('A', 'F'): 0.9 }
        
        self.assertEqual(set([('B', 'C'), ('D', 'E'),
                              ('E', 'F'), ('A', 'F')]),
                         minimum_spanning_tree(graph))

    def test_minimum_spanning_tree_synthetic_case(self):
        graph_100_nodes = synthetic_graph(100)
        minimum_spanning_tree(graph_100_nodes)

class IsSingleComponentTestCase(TestCase):
    def test_is_single_component_positive(self):
        graph = { ('A', 'B'): 0.1,
                  ('A', 'D'): 0.4,
                  ('B', 'C'): 0.2,
                  ('A', 'C'): 0.8,
                  ('D', 'E'): 0.2,
                  ('E', 'F'): 0.2,
                  ('A', 'F'): 0.9 }

        self.assertTrue(is_single_component(graph))

    def test_is_single_component_negative(self):
        graph = { ('A', 'B'): 0.1,
                  ('A', 'D'): 0.4,
                  ('B', 'C'): 0.2,
                  ('A', 'C'): 0.8,
                  ('E', 'F'): 0.2 }

        self.assertFalse(is_single_component(graph))

if __name__ == '__main__':
    unittest_main()
