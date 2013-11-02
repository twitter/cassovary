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

import com.twitter.cassovary.graph.{DeepEqualsNode, Node, NodeIdEdgesMaxId, StoredGraphDir}
import com.twitter.cassovary.graph.StoredGraphDir._
import org.specs.Specification

class ArrayBasedDirectedNodeSpec extends Specification {

  var onlyInNode: Node = _
  var onlyOutNode: Node = _
  var mutualNode: Node = _
  var bothNode: Node = _
  val nodeId = 100
  val neighbors = Array(1,2,3)
  val inEdges = Array(4,5)

  val small = beforeContext {
    onlyInNode = new Node {
      val id = nodeId
      def inboundNodes = neighbors
      val outboundNodes = Nil
    }
    onlyOutNode = new Node {
      val id = nodeId
      def outboundNodes = neighbors
      val inboundNodes = Nil
    }
    mutualNode = new Node {
      val id = nodeId
      def inboundNodes = neighbors
      def outboundNodes = neighbors
    }
    bothNode = new Node {
      val id = nodeId
      def inboundNodes = inEdges
      def outboundNodes = neighbors
    }
  }

  "array based directed node" definedAs small should {

    "constructs uni-directional nodes correctly" in {
      val nodeAndEdges = NodeIdEdgesMaxId(nodeId, neighbors)
      ArrayBasedDirectedNode(nodeAndEdges, StoredGraphDir.OnlyIn) must
          DeepEqualsNode(onlyInNode)
      ArrayBasedDirectedNode(nodeAndEdges, StoredGraphDir.OnlyOut) must
          DeepEqualsNode(onlyOutNode)
      ArrayBasedDirectedNode(nodeAndEdges, StoredGraphDir.Mutual) must
         DeepEqualsNode(mutualNode)
    }

    "constructs bi-directional nodes correctly" in {
      val nodeAndEdges = NodeIdEdgesMaxId(nodeId, neighbors)
      val node = ArrayBasedDirectedNode(nodeAndEdges, StoredGraphDir.BothInOut)
      node.asInstanceOf[BiDirectionalNode].inEdges = inEdges
      node must DeepEqualsNode(bothNode)
    }
  }
}
