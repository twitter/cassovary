package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph.DirectedGraph

/**
 * Created by bmckown on 1/28/15.
 */
abstract class AbstractCentrality(graph: DirectedGraph) extends Centrality {
  val centrality = new Array[Double](graph.maxNodeId + 1)
}
