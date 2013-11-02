/*
 * Copyright 2013 Twitter, Inc.
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

import com.twitter.cassovary.graph.{Node, StoredGraphDir}
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.SharedArraySeq
import com.twitter.cassovary.graph.LabeledNode

/**
 * Nodes in the graph that store edges in only one direction (or in the case Mutual Dir graph,
 * both directions have the same edges). Also the edges stored can not be mutated
 */
abstract class UniDirectionalLabeledNode private[graph] (val id: Int, val label: Int) extends LabeledNode

/**
 * Factory object for creating uni-directional labeled nodes that uses array as
 * underlying storage for node's edges
 */
object UniDirectionalLabeledNode {

  def apply(nodeId: Int, label: Int, neighbors: Array[Int], dir: StoredGraphDir) = {
    dir match {
      case StoredGraphDir.OnlyIn =>
        new UniDirectionalLabeledNode(nodeId, label) {
          def inboundNodes = neighbors
          def outboundNodes = Nil
        }
      case StoredGraphDir.OnlyOut =>
        new UniDirectionalLabeledNode(nodeId, label) {
          def inboundNodes = Nil
          def outboundNodes = neighbors
        }
      case StoredGraphDir.Mutual =>
        new UniDirectionalLabeledNode(nodeId, label) {
          def inboundNodes = neighbors
          def outboundNodes = neighbors
        }
    }
  }
}

/**
 * Factory object for creating uni-directional labeled nodes that uses shared array as underlying storage
 * for node's edges, * i.e. multiple nodes share a sharded two-dimensional array
 * object to hold its edges
 */
object SharedArrayBasedUniDirectionalLabeledNode {

  def apply(nodeId: Int, label: Int, edgeArrOffset: Int, edgeArrLen: Int, sharedArray: Array[Array[Int]],
      dir: StoredGraphDir) = {
    dir match {
      case StoredGraphDir.OnlyIn =>
        new UniDirectionalLabeledNode(nodeId, label) {
          def inboundNodes = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
          def outboundNodes = Nil
        }
      case StoredGraphDir.OnlyOut =>
        new UniDirectionalLabeledNode(nodeId, label) {
          def inboundNodes = Nil
          def outboundNodes = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
        }
      case StoredGraphDir.Mutual =>
        new UniDirectionalLabeledNode(nodeId, label) {
          def inboundNodes = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
          def outboundNodes = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
        }
    }
  }
}
