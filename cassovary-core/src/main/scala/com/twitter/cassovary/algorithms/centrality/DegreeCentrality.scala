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
import com.twitter.cassovary.graph.GraphDir.GraphDir
import collection.immutable.IndexedSeq

class DegreeCentrality(graph: DirectedGraph, dir: GraphDir, normalize: Boolean = true) extends AbstractCentrality(graph) {

  recalculate

  override def recalculate: IndexedSeq[Double] = {
    val denom = if (normalize) graph.nodeCount - 1 else 1.0
    graph foreach { node => centralityValues(node.id) = node.neighborCount(dir) / denom }
    centralityValues.toIndexedSeq
  }
}
