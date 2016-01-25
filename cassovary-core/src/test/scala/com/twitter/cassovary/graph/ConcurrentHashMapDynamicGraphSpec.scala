package com.twitter.cassovary.graph

import org.scalatest.{Matchers, WordSpec}

class ConcurrentHashMapDynamicGraphSpec extends WordSpec with Matchers {
  "An Efficient SynchronizedDynamicDirectedGraphSpec" should {
    "support adding nodes" in {
      val graph = new ConcurrentHashMapDynamicGraph()
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
      val graph = new ConcurrentHashMapDynamicGraph()
      graph.addEdge(1, 2)
      // For now, addEdge allows duplicates.  graph.addEdge(1, 2) // Test duplicate elimination
      graph.edgeCount shouldEqual 1
      val node1 = graph.getNodeById(1).get
      node1.inboundNodes.toSeq shouldEqual ( Seq())
      node1.outboundNodes.toSeq shouldEqual (Seq(2))
      val node2 = graph.getNodeById(2).get
      node2.inboundNodes.toSeq shouldEqual (Seq(1))
      node2.outboundNodes.toSeq shouldEqual (Seq())

      graph.addEdge(1, 10)
      graph.addEdge(200, 2)

      // Test multi-edge
      graph.addEdge(1, 2)
      node1.inboundNodes.toSeq shouldEqual (Seq())
      node1.outboundNodes.toSeq shouldEqual (Seq(2, 10, 2))
      node2.inboundNodes.toSeq shouldEqual (Seq(1, 200, 1))
      node2.outboundNodes.toSeq shouldEqual (Seq())
    }

    "support concurrent writing" in {
      val edgesPerThread = 10
      val threadCount = 8
      val graph = new ConcurrentHashMapDynamicGraph()
      val edgeAdders = (0 until threadCount) map { threadIndex =>
        new Thread() {
          override def run(): Unit = {
            for (i <- 0 until edgesPerThread) {
              graph.addEdge(0, threadIndex * edgesPerThread + i)
            }
          }
        }
      }
      val edgeReader = new Thread() {
        override def run(): Unit = {
          for (i <- 0 until 5) {
            val neighbors0 = graph.getOrCreateNode(0).outboundNodes
            Thread.sleep(1)
            assert(neighbors0.toSeq.count(_ == 0) <= 1)  // I expect the most common error would be
            // observing extra 0 entries
          }
        }
      }
      edgeReader.start()
      edgeAdders foreach (_.start())
      edgeAdders foreach (_.join())
      edgeReader.join()
      graph.getOrCreateNode(0).outboundNodes.toSeq should contain theSameElementsAs (0 until
        edgesPerThread * threadCount)

    }
  }
}
