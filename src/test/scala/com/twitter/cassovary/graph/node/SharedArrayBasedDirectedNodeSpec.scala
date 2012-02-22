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

import com.twitter.cassovary.graph.{DeepEqualsNode, Node, StoredGraphDir}
import com.twitter.cassovary.graph.StoredGraphDir._
import org.specs.Specification

class SharedArrayBasedDirectedNodeSpec extends Specification {

  var onlyInNode: Node = _
  var onlyOutNode: Node = _
  var mutualNode: Node = _
  var bothNode: Node = _
  val nodeId = 100
  val neighbors = Array(1,2,3)
  val inEdges = Array(4,5)
  val sharedArray = Array[Array[Int]](neighbors)

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
      SharedArrayBasedDirectedNode(nodeId, 0, 3, sharedArray,
          StoredGraphDir.OnlyIn) must DeepEqualsNode(onlyInNode)
      SharedArrayBasedDirectedNode(nodeId, 0, 3, sharedArray,
          StoredGraphDir.OnlyOut) must DeepEqualsNode(onlyOutNode)
      SharedArrayBasedDirectedNode(nodeId, 0, 3, sharedArray,
          StoredGraphDir.Mutual) must DeepEqualsNode(mutualNode)
    }

    "constructs bi-directional nodes correctly" in {
      val node = SharedArrayBasedDirectedNode(nodeId, 0, 3, sharedArray,
          StoredGraphDir.BothInOut, Some(inEdges))
      node must DeepEqualsNode(bothNode)
    }
  }
}
