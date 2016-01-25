/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.cassovary.graph.node

import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.Sharded2dArray
import com.twitter.cassovary.collections.CSeq

import com.twitter.cassovary.collections.CSeq.Implicits._

object SharedArrayBasedDirectedNode {
  /**
   * Creates a shared array based directed node (uni-directional or bi-directional) for
   * outgoing edges. Reverse edge arrays are not shared.
   *
   * @param nodeId an id of the node
   * @param edges a two-dimensional array that holds the underlying data for
   *                        the shared array based graph
   * @param dir the stored graph direction(s) (OnlyIn, OnlyOut, BothInOut or Mutual)
   * @param reverseDirEdges a shared reverse direction edge array
   *
   * @return a node
   */
  def apply(nodeId: Int, edges: Sharded2dArray[Int],
      dir: StoredGraphDir, reverseDirEdges: Option[Sharded2dArray[Int]] = None) = {
    dir match {
      case StoredGraphDir.OnlyIn | StoredGraphDir.OnlyOut | StoredGraphDir.Mutual =>
        val outEdges: CSeq[Int] = Option(edges(nodeId)).getOrElse(CSeq.empty[Int])
        UniDirectionalNode.applyCSeq(nodeId, outEdges, dir)
      case StoredGraphDir.BothInOut =>
        SharedArrayBasedBiDirectionalNode(nodeId, edges, reverseDirEdges.get)
    }
  }
}
