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

import collection.immutable
import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.graph.GraphDir.GraphDir

/**
 * Calculate the degree centrality for a specific graph and direction.  If normalization
 * is {@code true} then divide each node's degree centrality by the maximum number
 * of possible connections within the graph.
 * @param graph The graph object on which calculate the degree centrality
 * @param dir The direction of the edges to include in the degree centrality calculation.
 * @param normalize Pass true to normalize the values by the maximum number of possible connections.
 */
class DegreeCentrality(graph: DirectedGraph, dir: GraphDir, normalize: Boolean = true) extends AbstractCentrality(graph) {

  recalculate()

  def recalculate(): immutable.IndexedSeq[Double] = {
    val denom = if (normalize) graph.nodeCount - 1 else 1.0
    graph foreach { node => centralityValues(node.id) = node.neighborCount(dir) / denom }
    centralityValues.toIndexedSeq
  }
}
