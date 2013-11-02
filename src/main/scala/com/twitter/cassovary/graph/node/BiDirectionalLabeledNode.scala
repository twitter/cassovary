/*
 * Copyright 2012 Twitter, Inc.
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

import com.twitter.cassovary.graph.Node
import com.twitter.cassovary.util.SharedArraySeq
import com.twitter.cassovary.graph.LabeledNode

/**
 * Nodes in the graph that store both directions and
 * whose inEdges (and only inEdges) can be mutated after initialization
 */
abstract class BiDirectionalLabeledNode private[graph] (val id: Int, val label: Int) extends LabeledNode {
  var inEdges: Array[Int] = BiDirectionalNode.noNodes
  def inboundNodes = inEdges
}

object BiDirectionalLabeledNode {
  val noNodes = Array.empty[Int]

  def apply(nodeId: Int, label: Int, neighbors: Array[Int]) = {
    new BiDirectionalLabeledNode(nodeId, label) {
      def outboundNodes = neighbors
    }
  }
}

object SharedArrayBasedBiDirectionalLabeledNode {

  def apply(nodeId: Int, labelId: Int, edgeArrOffset: Int, edgeArrLen: Int, sharedArray: Array[Array[Int]],
      reverseDirEdgeArray: Array[Int]) = {
    new LabeledNode {
      val id = nodeId
      val label = labelId
      def outboundNodes = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
      def inboundNodes = reverseDirEdgeArray
    }
  }
}
