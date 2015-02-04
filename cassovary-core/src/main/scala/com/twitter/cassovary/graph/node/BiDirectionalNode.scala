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

import java.{util => jutil}

import com.twitter.cassovary.graph.{SortedNeighborsNodeOps, Node, SeqBasedNode}
import com.twitter.cassovary.util.SharedArraySeq

/**
 * Nodes in the graph that store both directions and
 * whose inEdges (and only inEdges) can be mutated after initialization
 */
trait BiDirectionalNode extends Node

case class FillingInEdgesBiDirectionalNode(id: Int, outboundNodes: Seq[Int])
  extends BiDirectionalNode {

  var inEdges: Array[Int] = null

  override def inboundNodes(): Seq[Int] = inEdges

  /**
   * Sorts incoming edges.
   */
  def sortInNeighbors() = {
    jutil.Arrays.sort(inEdges)
  }
}

object BiDirectionalNode {
  val noEdges = Array[Int]()

  def apply(nodeId: Int, out: Seq[Int], sortedNeighbors: Boolean): BiDirectionalNode = {
    if (sortedNeighbors) {
      new FillingInEdgesBiDirectionalNode(nodeId, out) with SortedNeighborsNodeOps
    } else {
      new FillingInEdgesBiDirectionalNode(nodeId, out)
    }
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

