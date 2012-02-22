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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.GraphDir._
import org.specs.Specification

class NodeUtilsSpec extends Specification {

  var node: Node = _

  val small = beforeContext {
    node = TestNode(100, List(1,2,3), List(60, 70))
  }

  "A node with 3 inEdges and 2 outEdges" definedAs small should {

    "respect hasTooManyEdges" in {
      NodeUtils.hasTooManyEdges(OutDir, None)(node) mustEqual false
      List(1,2,3,4) foreach { thresh =>
        NodeUtils.hasTooManyEdges(OutDir, Some(thresh))(node) mustEqual (thresh < 2)
      }
    }

    "handle removeSelfAndNodesDirectlyFollowing" in {
      NodeUtils.removeSelfAndNodesDirectlyFollowing(node,
          List(node), (nd: Node) => nd.id)mustEqual List()
      NodeUtils.removeSelfAndNodesDirectlyFollowing(node,
          List(0), (i: Int) => i) mustEqual List(0)
      val mixedListNodes = List(
          (60, TestNode(60, Nil, Nil)),
          (70, TestNode(70, Nil, Nil)),
          (80, TestNode(80, Nil, Nil)),
          (90, node)
        )
      NodeUtils.removeSelfAndNodesDirectlyFollowing(node,
          mixedListNodes, (x: (Int, Node)) => x._1) mustEqual
          List((80, TestNode(80, Nil, Nil)),(90, node))
    }

    "remove ids from a list" in {
      NodeUtils.removeFromList(Set(1, 2, 3, 4, 5), List((1,10), (20, 200), (4, 40), (10, 100)),
        (x: (Int, Int)) => x._1) mustEqual List((20, 200), (10, 100))
      NodeUtils.removeFromList[Int](Set(1, 2, 3, 4, 5), Nil, (x => x)) mustEqual Nil
    }
  }
}
