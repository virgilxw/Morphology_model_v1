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

def _euclidean(n, m):
    """helper for straightness"""
    return math.sqrt((n[0] - m[0]) ** 2 + (n[1] - m[1]) ** 2)

def _straightness_centrality(G, weight, normalized=True):
    """
    Calculates straightness centrality.
    """
    straightness_centrality = {}

    for n in G.nodes():
        straightness = 0
        sp = nx.single_source_dijkstra_path_length(G, n, weight=weight)

        if len(sp) > 0 and len(G) > 1:
            for target in sp:
                if n != target:
                    network_dist = sp[target]
                    euclidean_dist = _euclidean(n, target)
                    straightness = straightness + (euclidean_dist / network_dist)
            straightness_centrality[n] = straightness * (1.0 / (len(G) - 1.0))
            # normalize to number of nodes-1 in connected part
            if normalized:
                if len(sp) > 1:
                    s = (len(G) - 1.0) / (len(sp) - 1.0)
                    straightness_centrality[n] *= s
                else:
                    straightness_centrality[n] = 0
        else:
            straightness_centrality[n] = 0.0
    return straightness_centrality

def straightness_centrality_dask(
    graph,
    weight="mm_len",
    normalized=True,
    name="straightness",
    radius=None,
    distance=None,
    verbose=True,
    client = None
):
    """
    Calculates the straightness centrality for nodes.

    .. math::
        C_{S}(i)=\\frac{1}{n-1} \\sum_{j \\in V, j \\neq i} \\frac{d_{i j}^{E u}}{d_{i j}}

    where :math:`\\mathrm{d}^{\\mathrm{E} \\mathrm{u}}_{\\mathrm{ij}}` is the Euclidean distance
    between nodes `i` and `j` along a straight line.

    Adapted from :cite:`porta2006`.

    Parameters
    ----------
    graph : networkx.Graph
        Graph representing street network.
        Ideally generated from GeoDataFrame using :func:`momepy.gdf_to_nx`
    weight : str (default 'mm_len')
        attribute holding length of edge
    normalized : bool
        normalize to number of nodes-1 in connected part (for local straightness
        is recommended to set to normalized False)
    name : str, optional
        calculated attribute name
    radius: int
        Include all neighbors of distance <= radius from n
    distance : str, optional
        Use specified edge data key as distance.
        For example, setting ``distance=’weight’`` will use the edge ``weight`` to
        measure the distance from the node n during ego_graph generation.
    verbose : bool (default True)
        if True, shows progress bars in loops and indication of steps

    Returns
    -------
    Graph
        networkx.Graph

    Examples
    --------
    >>> network_graph = mm.straightness_centrality(network_graph)
    """
    netx = graph.copy()

    if radius:
        # Scatter 'netx' to all workers
        scattered_netx = client.scatter(netx, broadcast=True)

        # Define a function that takes the scattered 'netx'
        def compute_straightness_centrality(node, netx):
            sub = nx.ego_graph(netx, node, radius=radius, distance=distance)
            return _straightness_centrality(sub, weight=weight, normalized=normalized)[node]

        # Create a list of tasks using the scattered 'netx'
        tasks = [dask.delayed(compute_straightness_centrality)(n, scattered_netx) for n in netx]

        # Compute in parallel
        results = dask.compute(*tasks)

        # Assign results back to the nodes
        for n, result in zip(netx, results):
            netx.nodes[n][name] = result
    else:
        vals = _straightness_centrality(netx, weight=weight, normalized=normalized)
        nx.set_node_attributes(netx, vals, name)

    return netx