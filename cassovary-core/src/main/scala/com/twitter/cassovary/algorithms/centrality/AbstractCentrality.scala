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

import com.twitter.cassovary.graph.{Node, DirectedGraph}

abstract class AbstractCentrality(graph: DirectedGraph) extends Centrality {

  val centralityValues = new Array[Double](graph.maxNodeId + 1)

  /**
   * Get the specified node's centrality value
   * @param n Node
   * @return
   */
  def apply(n: Node): Double = centrality(n.id)

  def centrality: Array[Double]
}