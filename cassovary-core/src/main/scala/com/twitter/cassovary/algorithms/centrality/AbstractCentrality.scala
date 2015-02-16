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
import com.twitter.cassovary.graph.{Node, DirectedGraph}

/**
 * Centrality algorithm abstraction that requires a directed graph (``graph``) be passed upon
 * instantiation.
 * {@note The centrality values are stored in an internal array indexed by node's id. Hence, do not use for graphs with very large ids.}
 */
abstract class AbstractCentrality(graph: DirectedGraph[Node]) extends Centrality {

  protected val centralityValues = new Array[Double](graph.maxNodeId + 1)

  _recalc()
  /**
   * Get the specified node's centrality value
   * @param n Node
   * @return Centrality value for the n-th node
   */
  def apply(n: Node): Double = centralityValues(n.id)

  protected def _recalc(): Unit

  /**
   * Run the centrality calculation and update {@code centralityValues}
   * @return The centrality values of the graph
   */
  def recalculate(): immutable.IndexedSeq[Double] = {
    _recalc()
    centralityValues.toIndexedSeq
  }
}
