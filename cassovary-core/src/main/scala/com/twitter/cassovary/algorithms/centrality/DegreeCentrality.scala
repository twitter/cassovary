package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph.DirectedGraph

/**
 * Created by bmckown on 1/28/15.
 */

sealed abstract class DegreeCentrality(graph: DirectedGraph) extends AbstractCentrality(graph)

object InDegreeCentrality {
  def apply(graph: DirectedGraph): Array[Double] = {
    val idc = new InDegreeCentrality(graph)
    idc.run
  }
}

object OutDegreeCentrality {
  def apply(graph: DirectedGraph): Array[Double] = {
    val odc = new OutDegreeCentrality(graph)
    odc.run
  }
}

private class InDegreeCentrality(graph: DirectedGraph) extends AbstractCentrality(graph) {
  def run: Array[Double] = {

    graph.foreach { node =>
      centrality(node.id) = node.inboundCount
    }
    centrality
  }
}

private class OutDegreeCentrality(graph: DirectedGraph) extends DegreeCentrality(graph) {
  def run: Array[Double] = {
    graph.foreach { node =>
      centrality(node.id) = node.outboundCount
    }
    centrality
  }
}