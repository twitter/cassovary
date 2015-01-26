/*
 * Copyright (c) 2014. Twitter, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
 * file except in compliance with the License. You may obtain a copy of the License at
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */
package com.twitter.cassovary.graph.node

import NodeTestUtils._
import com.twitter.cassovary.graph.Node
import org.scalatest.{Matchers, WordSpec}

trait NodeBehaviors extends WordSpec with Matchers {
  val nodeId = 100
  val neighbors = Array(1,2,3)
  val inEdges = Array(4,5)

  def correctlyConstructNodes(actualOnlyIn: Node, actualOnlyOut: Node, actualMutual: Node,
                              actualBoth: Node): Unit = {
    "construct uni-directional nodes correctly" in {
      val onlyInNode = Node(nodeId, out = Nil, neighbors)
      val onlyOutNode = Node(nodeId, out = neighbors, Nil)
      val mutualNode = Node(nodeId, out = neighbors, neighbors)
      actualOnlyIn should deepEquals(onlyInNode)
      actualOnlyOut should deepEquals(onlyOutNode)
      actualMutual should deepEquals(mutualNode)
    }

    "constructs bi-directional nodes correctly" in {
      val bothNode = Node(nodeId, out = neighbors, in = inEdges)
      actualBoth should deepEquals(bothNode)
    }
  }
}
