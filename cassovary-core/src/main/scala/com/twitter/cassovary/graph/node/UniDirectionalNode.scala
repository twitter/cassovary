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

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.graph.{NeighborsInCSeqNode, NeighborsInArrayNode, Node, SortedNeighborsNodeOps}
import com.twitter.cassovary.collections.CSeq

import com.twitter.cassovary.collections.CSeq.Implicits._

/**
 * Nodes in the graph that store edges in only one direction (or in the case Mutual Dir graph,
 * both directions have the same edges). Also the edges stored can not be mutated
 */
trait UniDirectionalNode extends Node

/**
 * Factory object for creating uni-directional nodes that uses array as underlying storage
 * for node's edges
 */
object UniDirectionalNode {
  private val emptyArray = Array.empty[Int]

  def apply(nodeId: Int, neighbors: Array[Int], dir: StoredGraphDir,
      sortedNeighbors: Boolean = false) = {
    val in = if (dir == OnlyOut) emptyArray else neighbors
    val out = if (dir == OnlyIn) emptyArray else neighbors
    if (sortedNeighbors) {
      new NeighborsInArrayNode(nodeId, in, out) with UniDirectionalNode with SortedNeighborsNodeOps
    } else {
      new NeighborsInArrayNode(nodeId, in, out) with UniDirectionalNode
    }
  }

  def applyCSeq(nodeId: Int, neighbors: CSeq[Int], dir: StoredGraphDir,
                sortedNeighbors: Boolean = false) = {
    val in = if (dir == OnlyOut) CSeq.empty[Int] else neighbors
    val out = if (dir == OnlyIn) CSeq.empty[Int] else neighbors
    if (sortedNeighbors) {
      new NeighborsInCSeqNode(nodeId, in, out) with UniDirectionalNode with SortedNeighborsNodeOps
    } else {
      new NeighborsInCSeqNode(nodeId, in, out) with UniDirectionalNode
    }
  }

  @deprecated("Use apply instead.", "7.0.0")
  def applySeq(nodeId: Int, neighbors: Seq[Int], dir: StoredGraphDir,
      sortedNeighbors: Boolean = false) = {
    apply(nodeId, neighbors.toArray, dir, sortedNeighbors)
  }

}

/**
 * A node where both directions have the same edges.
 */
trait UndirectedNode extends UniDirectionalNode {
  final def inboundNodes() = outboundNodes()
}

object UndirectedNode {
  def apply(nodeId: Int, outbound: Seq[Int]) =
    new UndirectedNode {
      override val id = nodeId
      override def outboundNodes() = outbound
    }
}

/**
 * Factory object for creating uni-directional nodes that uses shared array as underlying storage
 * for node's edges, * i.e. multiple nodes share a shared two-dimensional array
 * object to hold its edges
 */
object SharedArrayBasedUniDirectionalNode {

  def apply(nodeId: Int, edgeArrOffset: Int, edgeArrLen: Int, sharedArray: Array[Array[Int]],
            dir: StoredGraphDir) = {
    val neighbors = CSeq[Int](sharedArray(nodeId % sharedArray.length), edgeArrOffset,
      edgeArrOffset + edgeArrLen)
    new UniDirectionalNode {
      val id = nodeId

      def inboundNodes() = if (dir == OnlyOut) CSeq.empty[Int] else neighbors

      def outboundNodes() = if (dir == OnlyIn) CSeq.empty[Int] else neighbors
    }

  }
}
