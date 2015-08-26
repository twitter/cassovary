package com.twitter.cassovary.graph


import org.scalatest.{Matchers, WordSpec}

class SemiSynchronizedDynamicGraphSpec extends WordSpec with Matchers {
  "A SemiSynchronizedDynamicDirectedGraphSpec" should {
    "support adding nodes" in {
      val graph = new SemiSynchronizedDynamicGraph()
      for (i <- 0 until 3) {
        graph.nodeCount shouldEqual i
        graph.edgeCount shouldEqual 0
        graph.getOrCreateNode(10 * i) // non-contiguous
      }
      graph.getOrCreateNode(10) // Accessing again should increase node count
      graph.nodeCount shouldEqual 3
      graph.existsNodeId(1000000) shouldEqual false
    }

    "support adding edges" in {
      val graph = new SemiSynchronizedDynamicGraph()
      graph.addEdge(1, 2)
      // For now, addEdge allows duplicates.  graph.addEdge(1, 2) // Test duplicate elimination
      graph.edgeCount shouldEqual 1
      val node1 = graph.getNodeById(1).get
      node1.inboundNodes.toList shouldEqual ( List())
      node1.outboundNodes.toList shouldEqual (List(2))
      val node2 = graph.getNodeById(2).get
      node2.inboundNodes.toList shouldEqual (List(1))
      node2.outboundNodes.toList shouldEqual (List())

      // test immutability of outboundNodes
      val oldOutboundNodes1: Seq[Int] = node1.outboundNodes()
      val oldInboundNodes2: Seq[Int] = node2.inboundNodes()
      graph.addEdge(1, 10)
      graph.addEdge(200, 2)
      oldOutboundNodes1.toList shouldEqual (List(2))
      oldInboundNodes2.toList shouldEqual (List(1))

      // Test multi-edge
      graph.addEdge(1, 2)
      node1.inboundNodes.toList shouldEqual (List())
      node1.outboundNodes.toList shouldEqual (List(2, 10, 2))
      node2.inboundNodes.toList shouldEqual (List(1, 200, 1))
      node2.outboundNodes.toList shouldEqual (List())
    }
  }
}
