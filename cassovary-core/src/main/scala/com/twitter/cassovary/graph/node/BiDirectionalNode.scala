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

import com.twitter.cassovary.graph.{Node, SeqBasedNode, SortedNeighborsNodeOps}
import com.twitter.cassovary.util.SharedArraySeq
import scala.collection.mutable

/**
 * Nodes in the graph that store both directions and
 * whose inEdges (and only inEdges) can be mutated after initialization
 */
trait BiDirectionalNode extends Node

case class FillingInEdgesBiDirectionalNode(id: Int, inEdgeSize: Int,
                                           outboundNodes: Seq[Int])
  extends BiDirectionalNode {

  def this(node: Node, inEdgesSize: Int) =
    this(node.id, inEdgesSize, node.outboundNodes())

  val inEdges = new Array[Int](inEdgeSize)

  override def inboundNodes(): Seq[Int] = inEdges

  /**
   * Finishes filling incoming edges of this node.
   *
   * Sorts them if `sortNeighbors` is true.
   */
  def finishingFilling(sortNeighbors: Boolean): BiDirectionalNode = {
    if (sortNeighbors) {
      BiDirectionalNode(id, inEdges.sorted, outboundNodes, true)
    } else {
      BiDirectionalNode(id, inEdges, outboundNodes)
    }
  }
}

object BiDirectionalNode {
  val noEdges = Array[Int]()

  def apply(nodeId: Int, out: Seq[Int], sortedNeighbors: Boolean): BiDirectionalNode = {
    BiDirectionalNode(nodeId, noEdges, out, sortedNeighbors)
  }

  def apply(nodeId: Int, in: Seq[Int], out: Seq[Int], sortedNeighbors: Boolean = false): BiDirectionalNode = {
    if (sortedNeighbors) {
      new SeqBasedNode(nodeId, in, out) with BiDirectionalNode with SortedNeighborsNodeOps
    } else {
      new SeqBasedNode(nodeId, in, out) with BiDirectionalNode
    }
  }
}

object SharedArrayBasedBiDirectionalNode {
  def apply(nodeId: Int, edgeArrOffset: Int, edgeArrLen: Int, sharedArray: Array[Array[Int]],
      reverseDirEdgeArray: Array[Int]) = {
    new Node {
      val id = nodeId
      def outboundNodes() = new SharedArraySeq(nodeId, sharedArray, edgeArrOffset, edgeArrLen)
      def inboundNodes() = reverseDirEdgeArray
    }
  }
}

