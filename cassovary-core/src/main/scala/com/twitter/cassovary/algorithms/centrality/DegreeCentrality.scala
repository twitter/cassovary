/*
 * Copyright 2015 Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.graph.DirectedGraph

sealed abstract class DegreeCentrality(graph: DirectedGraph) extends AbstractCentrality(graph) {

  /**
   * Normalize the values
   * @return
   */
  def normalize(normalize: Boolean = true): Array[Double] = {
    if (normalize)
      centrality map { c => c / (graph.maxNodeId - 1) }
    else
      centrality
  }
}

object InDegreeCentrality {

  /**
   * Determines the in-degree centrality of each node in the graph.  Currently, we only support
   * calculating the IDC for each member of the graph and not for the graph on the whole.  Although this
   * functionality would not be terribly difficult to add.
   * @param graph A DirectedGraph instance
   * @return
   */
  def apply(graph: DirectedGraph, normalize: Boolean = true): Array[Double] = {
    val idc = new InDegreeCentrality(graph)
    idc.centrality
    idc.normalize(normalize)
  }
}

object OutDegreeCentrality {

  /**
   * Determines the out-degree centrality of each node in the graph.  Currently, we only support
   * calculating the ODC for each member of the graph and not for the graph on the whole.  Although this
   * functionality would not be terribly difficult to add.
   * @param graph A DirectedGraph instance
   * @return
   */
  def apply(graph: DirectedGraph, normalize: Boolean = true): Array[Double] = {
    val odc = new OutDegreeCentrality(graph)
    odc.centrality
    odc.normalize(normalize)
  }
}

private class InDegreeCentrality(graph: DirectedGraph) extends DegreeCentrality(graph) {

  /**
   * Run the indegree centrality calculation for the graph.
   * @return
   */
  def centrality: Array[Double] = {
    graph foreach { n => centralityValues(n.id) = n.inboundCount }
    centralityValues
  }
}

private class OutDegreeCentrality(graph: DirectedGraph) extends DegreeCentrality(graph) {

  /**
   * Run the outdegree centrality calculation for the graph.
   * @return
   */
  def centrality: Array[Double] = {
    graph foreach { n => centralityValues(n.id) = n.outboundCount }
    centralityValues
  }
}
