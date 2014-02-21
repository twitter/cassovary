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

import com.twitter.cassovary.graph.Node
import com.twitter.cassovary.util.SharedArraySeq

/**
 * Nodes in the graph that store both directions and
 * whose inEdges (and only inEdges) can be mutated after initialization
 */
abstract class BiDirectionalNode private[graph] (val id: Int) extends Node {
  var inEdges: Array[Int] = BiDirectionalNode.noNodes
  def inboundNodes = inEdges
}

object BiDirectionalNode {
  val noNodes = Array.empty[Int]

  def apply(nodeId: Int, neighbors: Array[Int]) = {
    new BiDirectionalNode(nodeId) {
      def outboundNodes = neighbors
    }
  }
}

object SharedArrayBasedBiDirectionalNode {

  def apply(nodeId: Int, edgeArrOffset: Int, edgeArrLen: Int, sharedArray: Array[Array[Int]],
      reverseDirEdgeArray: Array[Int]) = {
    new Node {
      val id = nodeId
      def outboundNodes = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
      def inboundNodes = reverseDirEdgeArray
    }
  }
}
