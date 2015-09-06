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

object ArrayBasedDirectedNode {
  val noNodes = Array.empty[Int]
  /**
   * creates an array based directed node (uni-directional or bi-directional)
   * @param nodeId an id of the node
   * @param neighbors a seq of ids of the neighbors read from file
   * @param dir the stored graph direction (OnlyIn, OnlyOut, BothInOut or Mutual)
   * @param sortedNeighbors true if the neighbors of the node will be sorted
   *
   * @return a node
   */
  def apply(nodeId: Int, neighbors: Array[Int], dir: StoredGraphDir, sortedNeighbors: Boolean = false) = {
    dir match {
      case StoredGraphDir.OnlyIn | StoredGraphDir.OnlyOut | StoredGraphDir.Mutual =>
        UniDirectionalNode(nodeId, neighbors, dir, sortedNeighbors)
      case StoredGraphDir.BothInOut =>
        FillingInEdgesBiDirectionalNode(nodeId, neighbors, sortedNeighbors)
    }
  }
}
