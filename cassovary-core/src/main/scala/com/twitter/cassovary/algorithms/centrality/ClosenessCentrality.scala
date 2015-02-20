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

import com.twitter.cassovary.graph._

/**
 * Calculate the closeness centrality of ``graph``
 * @param normalize Normalize by the ratio of number of connections to other nodes
 *                  and the number of nodes in the graph
 */
class ClosenessCentrality(graph: DirectedGraph[Node], normalize: Boolean = true) extends AbstractCentrality(graph) {

  def _recalc(): Unit = {
    graph foreach { node =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits())
      val (sum, reachableNodes) = bfs.foldLeft((0.0, 0.0)) {
        case ((partialSum, partialCount), a) => (partialSum + bfs.depth(a.id).get, partialCount + 1)
      }
      centralityValues(node.id) = if (sum > 0 && graph.nodeCount > 1) {
          val numerator = reachableNodes - 1
          val denom = if (normalize) (graph.nodeCount - 1) / (reachableNodes - 1) else 1
          numerator / (denom * sum)
      }
      else 0.0
    }
  }
}
