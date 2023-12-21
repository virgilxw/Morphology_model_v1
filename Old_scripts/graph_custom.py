#!/usr/bin/env python
# -*- coding: utf-8 -*-

# connectivity.py
# definitions of connectivity characters
import math
import warnings

import networkx as nx
import numpy as np
from tqdm import tqdm

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

def _closeness_centrality(G, u=None, length=None, wf_improved=True, len_graph=None):
    r"""Compute closeness centrality for nodes. Slight adaptation of networkx
    `closeness_centrality` to allow normalisation for local closeness.
    Adapted script used in networkx.

    Closeness centrality [1]_ of a node `u` is the reciprocal of the
    average shortest path distance to `u` over all `n-1` reachable nodes.

    .. math::

        C(u) = \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)},

    where `d(v, u)` is the shortest-path distance between `v` and `u`,
    and `n` is the number of nodes that can reach `u`. Notice that the
    closeness distance function computes the incoming distance to `u`
    for directed graphs. To use outward distance, act on `G.reverse()`.

    Notice that higher values of closeness indicate higher centrality.

    Wasserman and Faust propose an improved formula for graphs with
    more than one connected component. The result is "a ratio of the
    fraction of actors in the group who are reachable, to the average
    distance" from the reachable actors [2]_. You might think this
    scale factor is inverted but it is not. As is, nodes from small
    components receive a smaller closeness value. Letting `N` denote
    the number of nodes in the graph,

    .. math::

        C_{WF}(u) = \frac{n-1}{N-1} \frac{n - 1}{\sum_{v=1}^{n-1} d(v, u)},

    Parameters
    ----------
    G : graph
      A NetworkX graph

    u : node, optional
      Return only the value for node u

    distance : edge attribute key, optional (default=None)
      Use the specified edge attribute as the edge distance in shortest
      path calculations

    len_graph : int
        length of complete graph

    Returns
    -------
    nodes : dictionary
      Dictionary of nodes with closeness centrality as the value.

    References
    ----------
    .. [1] Linton C. Freeman: Centrality in networks: I.
       Conceptual clarification. Social Networks 1:215-239, 1979.
       http://leonidzhukov.ru/hse/2013/socialnetworks/papers/freeman79-centrality.pdf
    .. [2] pg. 201 of Wasserman, S. and Faust, K.,
       Social Network Analysis: Methods and Applications, 1994,
       Cambridge University Press.
    """

    if length is not None:
        import functools

        # use Dijkstra's algorithm with specified attribute as edge weight
        path_length = functools.partial(
            nx.single_source_dijkstra_path_length, weight=length
        )
    else:
        path_length = nx.single_source_shortest_path_length

    nodes = [u]
    closeness_centrality = {}
    for n in nodes:
        sp = dict(path_length(G, n))
        totsp = sum(sp.values())
        if totsp > 0.0 and len(G) > 1:
            closeness_centrality[n] = (len(sp) - 1.0) / totsp
            # normalize to number of nodes-1 in connected part
            s = (len(sp) - 1.0) / (len_graph - 1)
            closeness_centrality[n] *= s
        else:
            closeness_centrality[n] = 0.0

    return closeness_centrality[u]

def closeness_centrality(
    ego_graph_collection,
    graph,
    name="closeness",
    weight="mm_len",
    radius=None,
    distance=None,
    verbose=True,
    **kwargs
):
    """
    Calculates the closeness centrality for nodes.

    Wrapper around ``networkx.closeness_centrality``.

    Closeness centrality of a node `u` is the reciprocal of the
    average shortest path distance to `u` over all `n-1` nodes within reachable nodes.

    .. math::

        C(u) = \\frac{n - 1}{\\sum_{v=1}^{n-1} d(v, u)},

    where :math:`d(v, u)` is the shortest-path distance between :math:`v` and :math:`u`,
    and :math:`n` is the number of nodes that can reach :math:`u`.


    Parameters
    ----------
    graph : networkx.Graph
        Graph representing street network.
        Ideally generated from GeoDataFrame using :func:`momepy.gdf_to_nx`
    name : str, optional
        calculated attribute name
    weight : str (default 'mm_len')
        attribute holding the weight of edge (e.g. length, angle)
    radius: int
        Include all neighbors of distance <= radius from n
    distance : str, optional
        Use specified edge data key as distance.
        For example, setting ``distance=’weight’`` will use the edge ``weight`` to
        measure the distance from the node n during ego_graph generation.
    verbose : bool (default True)
        if True, shows progress bars in loops and indication of steps
    **kwargs
        kwargs for ``networkx.closeness_centrality``

    Returns
    -------
    Graph
        networkx.Graph

    Examples
    --------
    >>> network_graph = mm.closeness_centrality(network_graph)
    """
    netx = graph.copy()

    if radius:
        lengraph = len(netx)
        for n in tqdm(ego_graph_collection, total=len(ego_graph_collection), disable=not verbose):
            sub = ego_graph_collection[n]
            netx.nodes[n][name] = _closeness_centrality(
                sub, n, length=weight, len_graph=lengraph
            )
    else:
        vals = nx.closeness_centrality(netx, distance=weight, **kwargs)
        nx.set_node_attributes(netx, vals, name)

    return netx