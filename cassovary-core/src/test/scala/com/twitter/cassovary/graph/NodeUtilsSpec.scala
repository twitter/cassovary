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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.GraphDir._
import org.scalatest.{Matchers, WordSpec}

class NodeUtilsSpec extends WordSpec with Matchers  {

  def fixture = TestNode(100, List(1,2,3), List(60, 70))

  "NodeUtils" when {
    "given a node having 3 inEdges and 2 outEdges" should {
      "respect hasTooManyEdges" in {
        val node = fixture
        NodeUtils.hasTooManyEdges(OutDir, None)(node) shouldEqual false
        List(1, 2, 3, 4) foreach { thresh =>
          NodeUtils.hasTooManyEdges(OutDir, Some(thresh))(node) shouldEqual (thresh < 2)
        }
      }
      "handle removeSelfAndNodesDirectlyFollowing" in {
        val node = fixture
        NodeUtils.removeSelfAndNodesDirectlyFollowing(node,
          List(node), (nd: Node) => nd.id) shouldEqual List()
        NodeUtils.removeSelfAndNodesDirectlyFollowing(node,
          List(0), (i: Int) => i) shouldEqual List(0)
        val mixedListNodes = List(
          (60, TestNode(60, Nil, Nil)),
          (70, TestNode(70, Nil, Nil)),
          (80, TestNode(80, Nil, Nil)),
          (90, node)
        )
        NodeUtils.removeSelfAndNodesDirectlyFollowing(node,
          mixedListNodes, (x: (Int, Node)) => x._1) shouldEqual
          List((80, TestNode(80, Nil, Nil)), (90, node))
      }
    }
    "without any given node" should {
      "remove ids from a list" in {
        NodeUtils.removeFromList(Set(1, 2, 3, 4, 5), List((1, 10), (20, 200), (4, 40), (10, 100)),
          (x: (Int, Int)) => x._1) shouldEqual List((20, 200), (10, 100))
        NodeUtils.removeFromList[Int](Set(1, 2, 3, 4, 5), Nil, x => x) shouldEqual Nil
      }
    }
  }
}
