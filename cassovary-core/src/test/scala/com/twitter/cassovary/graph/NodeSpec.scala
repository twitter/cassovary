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

import org.scalatest.{Matchers, WordSpec}

class NodeSpec extends WordSpec with Matchers {

  def noInboundOrOutboundEdges = TestNode(1, Nil, Nil)
  def onlyInboundEdges = TestNode(1, List(2), Nil)
  def onlyOutboundEdges = TestNode(1, Nil, List(2))
  def inboundAndOutboundEdges = TestNode(1, List(2), List(3))

  def noInboundEdges(node: Node): Unit = {
    "have no inbound edges" in {
      node.inboundCount shouldEqual 0
      node.inboundNodes().size shouldEqual 0
      node.isInboundNode(1) shouldEqual false
    }

    "produce no random inbound edges" in {
      node.randomInboundNode should be (None)
      node.randomNeighborSet(4, GraphDir.InDir).size shouldEqual 0
    }
  }

  def noOutboundEdges(node: Node): Unit = {
    "have no outbound edges" in {
      node.outboundCount shouldEqual 0
      node.outboundNodes().size shouldEqual 0
      node.isOutboundNode(1) shouldEqual false
    }

    "produce no random outbound edges" in {
      node.randomOutboundNode should be (None)
      node.randomNeighborSet(4, GraphDir.OutDir).size shouldEqual 0
    }
  }

  "A node with no inbound or outbound edges" should {
    val node = noInboundOrOutboundEdges
    behave like noInboundEdges(node)
    behave like noOutboundEdges(node)
  }

  "A node with only inbound edges" should {
    "have an inbound edge" in {
      val node = onlyInboundEdges
      node.inboundCount shouldEqual 1
      node.inboundNodes should contain(2)
      node.isInboundNode(1) shouldEqual false
      node.isInboundNode(2) shouldEqual true
    }

    "produce a random inbound edge" in {
      val node = onlyInboundEdges
      node.randomInboundNode shouldEqual Some(2)
      node.randomNeighborSet(4, GraphDir.InDir).size shouldEqual 4
      node.randomNeighborSet(4, GraphDir.InDir)(0) shouldEqual 2
    }

    behave like noOutboundEdges(onlyInboundEdges)
  }


  "A node with only outbound edges" should {
    behave like noInboundEdges(onlyOutboundEdges)

    "have an outbound edge" in {
      val node = onlyOutboundEdges
      node.outboundCount shouldEqual 1
      node.outboundNodes should contain (2)
      node.isOutboundNode(1) shouldEqual false
      node.isOutboundNode(2) shouldEqual true
    }

    "produce a random outbound edge" in {
      val node = onlyOutboundEdges
      node.randomOutboundNode shouldEqual Some(2)
      node.randomNeighborSet(4, GraphDir.OutDir).size shouldEqual 4
      node.randomNeighborSet(4, GraphDir.OutDir)(0) shouldEqual 2
    }
  }

  "A node with inbound and outbound edges" should {
    "have an inbound edge" in {
      val node = inboundAndOutboundEdges
      node.inboundCount shouldEqual 1
      node.inboundNodes should contain (2)
      node.isInboundNode(1) shouldEqual false
      node.isInboundNode(2) shouldEqual true
      node.isInboundNode(3) shouldEqual false
    }

    "produce a random inbound edge" in {
      val node = inboundAndOutboundEdges
      node.randomInboundNode shouldEqual Some(2)
      node.randomNeighborSet(4, GraphDir.InDir).size shouldEqual 4
      node.randomNeighborSet(4, GraphDir.InDir)(0) shouldEqual 2
    }

    "have an outbound edge" in {
      val node = inboundAndOutboundEdges
      node.outboundCount shouldEqual 1
      node.outboundNodes should contain (3)
      node.isOutboundNode(1) shouldEqual false
      node.isOutboundNode(2) shouldEqual false
      node.isOutboundNode(3) shouldEqual true
    }

    "produce a random outbound edge" in {
      val node = inboundAndOutboundEdges
      node.randomOutboundNode shouldEqual Some(3)
      node.randomNeighborSet(4, GraphDir.OutDir).size shouldEqual 4
      node.randomNeighborSet(4, GraphDir.OutDir)(0) shouldEqual 3
    }
  }
}
