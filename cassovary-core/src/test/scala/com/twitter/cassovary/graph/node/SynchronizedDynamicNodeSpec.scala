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

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class SynchronizedDynamicNodeSpec extends WordSpec with ShouldMatchers {

  def fixture(id: Int) = new SynchronizedDynamicNode(id)

  "Two SynchronizedDynamicNodes equal iff their nodeIds are equal" in {
    val node1 = fixture(1)
    val node2 = fixture(1)
    node1.addInBoundNode(2)
    val node3 = fixture(2)
    node1 shouldEqual node2
    node1 == node3 shouldEqual false
  }

  "add/delete functions perform correctly" in {
    val node = fixture(1)
    node.addInBoundNode(2)
    node.addInBoundNode(3)
    node.inboundNodes.toList shouldEqual List(2, 3)
    node.removeInBoundNode(3)
    node.removeInBoundNode(4) // non-existing items won't throw exceptions
    node.inboundNodes.toList shouldEqual List(2)

    node.addOutBoundNode(2)
    node.addOutBoundNode(3)
    node.outboundNodes.toList shouldEqual List(2, 3)
    node.removeOutBoundNode(3)
    node.removeOutBoundNode(4)
    node.outboundNodes.toList shouldEqual List(2)
  }
}

