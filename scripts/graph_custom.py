#!/usr/bin/env python
# -*- coding: utf-8 -*-

# connectivity.py
# definitions of connectivity characters
import math
import warnings

import networkx as nx
import numpy as np
from tqdm import tqdm
import dask
import networkx as nx
from dask import compute

from dask.distributed import Client, LocalCluster

from dask import delayed, compute

@delayed
def generate_subgraph(netx, node, radius, distance):
    return node, nx.ego_graph(netx, node, radius=radius, distance=distance)

def generate_ego_graph(graph, radius=5, distance=None, verbose=True, client = None):
    netx = graph.copy()
    delayed_subgraphs = []

    
    netx_scattered = client.scatter(netx, broadcast=True)

    for n in tqdm(netx, total=len(netx), disable=not verbose):
        delayed_subgraphs.append(delayed(generate_subgraph)(netx_scattered, n, radius, distance))

    out = dict(compute(*delayed_subgraphs))
    return out
