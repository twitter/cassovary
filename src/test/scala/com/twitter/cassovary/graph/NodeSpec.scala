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

import org.specs.Specification

class NodeSpec extends Specification {

  var node: Node = _

  val noInboundOrOutboundEdges = beforeContext(node = TestNode(1, Nil, Nil))
  val onlyInboundEdges = beforeContext(node = TestNode(1, List(2), Nil))
  val onlyOutboundEdges = beforeContext(node = TestNode(1, Nil, List(2)))
  val inboundAndOutboundEdges = beforeContext(node = TestNode(1, List(2), List(3)))
  val noInboundOrOutboundEdgesMutable = beforeContext(node = TestMutableNode(1, Nil, Nil))

  def noInboundEdges = {
    "have no inbound edges" in {
      node.inboundCount mustBe 0
      node.inboundNodes.size mustBe 0
      node.isInboundNode(1) mustBe false
    }

    "produce no random inbound edges" in {
      node.randomInboundNode must beNone
      node.randomNeighborSet(4, GraphDir.InDir).size mustBe 0
    }
  }

  def noOutboundEdges = {
    "have no outbound edges" in {
      node.outboundCount mustBe 0
      node.outboundNodes.size mustBe 0
      node.isOutboundNode(1) mustBe false
    }

    "produce no random outbound edges" in {
      node.randomOutboundNode must beNone
      node.randomNeighborSet(4, GraphDir.OutDir).size mustBe 0
    }
  }

  "A node with no inbound or outbound edges" definedAs noInboundOrOutboundEdges should {
    "lack any edges" in {
      noInboundEdges
      noOutboundEdges
    }
  }

  "A node with only inbound edges" definedAs onlyInboundEdges should {
    "have an inbound edge" in {
      node.inboundCount mustBe 1
      node.inboundNodes must contain(2)
      node.isInboundNode(1) mustBe false
      node.isInboundNode(2) mustBe true
    }

    "produce a random inbound edge" in {
      node.randomInboundNode must equalTo(Some(2))
      node.randomNeighborSet(4, GraphDir.InDir).size mustBe 4
      node.randomNeighborSet(4, GraphDir.InDir)(0) mustBe 2
    }

    "lack any outbound edges" in { noOutboundEdges }
  }


  "A node with only outbound edges" definedAs onlyOutboundEdges should {
    "lack any inbound edges" in { noInboundEdges }

    "have an outbound edge" in {
      node.outboundCount mustBe 1
      node.outboundNodes must contain(2)
      node.isOutboundNode(1) mustBe false
      node.isOutboundNode(2) mustBe true
    }

    "produce a random outbound edge" in {
      node.randomOutboundNode must equalTo(Some(2))
      node.randomNeighborSet(4, GraphDir.OutDir).size mustBe 4
      node.randomNeighborSet(4, GraphDir.OutDir)(0) mustBe 2
    }
  }

  "A node with inbound and outbound edges" definedAs inboundAndOutboundEdges should {
    "have an inbound edge" in {
      node.inboundCount mustBe 1
      node.inboundNodes must contain(2)
      node.isInboundNode(1) mustBe false
      node.isInboundNode(2) mustBe true
      node.isInboundNode(3) mustBe false
    }

    "produce a random inbound edge" in {
      node.randomInboundNode must be equalTo(Some(2))
      node.randomNeighborSet(4, GraphDir.InDir).size mustBe 4
      node.randomNeighborSet(4, GraphDir.InDir)(0) mustBe 2
    }

    "have an outbound edge" in {
      node.outboundCount mustBe 1
      node.outboundNodes must contain(3)
      node.isOutboundNode(1) mustBe false
      node.isOutboundNode(2) mustBe false
      node.isOutboundNode(3) mustBe true
    }

    "produce a random outbound edge" in {
      node.randomOutboundNode must equalTo(Some(3))
      node.randomNeighborSet(4, GraphDir.OutDir).size mustBe 4
      node.randomNeighborSet(4, GraphDir.OutDir)(0) mustBe 3
    }
  }

  "A node with a mutable id" definedAs noInboundOrOutboundEdgesMutable should {
    "be able to modify its id" in {
      node.id mustEqual 1
      node.asInstanceOf[TestMutableNode].id = 2
      node.id mustEqual 2
    }
  }

}
