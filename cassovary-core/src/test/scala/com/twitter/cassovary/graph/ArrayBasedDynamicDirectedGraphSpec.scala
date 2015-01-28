package com.twitter.cassovary.graph

import org.scalatest.{Matchers, WordSpec}
import com.twitter.cassovary.graph.StoredGraphDir._

class ArrayBasedDynamicDirectedGraphSpec extends WordSpec with Matchers with GraphBehaviours {
  val graphDirections = List(StoredGraphDir.OnlyIn, StoredGraphDir.OnlyOut, StoredGraphDir.BothInOut,
                             StoredGraphDir.Mutual) // Bipartitie is not supported

  def builder(iteratable: Iterable[NodeIdEdgesMaxId],
              storedGraphDir: StoredGraphDir) =
    new ArrayBasedDynamicDirectedGraph(iteratable, storedGraphDir)
  verifyGraphBuilding(builder, sampleGraphEdges)

  "An ArrayBasedDynamicDirectedGraph" should {
    "support adding nodes" in {
      val graph = new ArrayBasedDynamicDirectedGraph(StoredGraphDir.BothInOut)
      for (i <- 0 until 3) {
        graph.nodeCount shouldEqual i
        graph.edgeCount shouldEqual 0
        graph.getOrCreateNode(10 * i) // non-contiguous
      }
      graph.getOrCreateNode(10) // Accessing again should increase node count
      graph.nodeCount shouldEqual 3
      graph.nodeExists(1000000) shouldEqual false
    }


    "support adding edges" in {
      for (dir <- graphDirections) {
        val graph = new ArrayBasedDynamicDirectedGraph(dir)
        val inStored = graph.isDirStored(GraphDir.InDir)
        val outStored = graph.isDirStored(GraphDir.OutDir)
        val notMutual = dir != StoredGraphDir.Mutual

        graph.addEdge(1, 2)
        graph.addEdge(1, 2) // Test duplicate elimination
        graph.edgeCount shouldEqual (if (notMutual) 1 else 2)
        val node1 = graph.getNodeById(1).get
        node1.inboundNodes.toList shouldEqual (if(notMutual) List() else List(2))
        node1.outboundNodes.toList shouldEqual (if(outStored) List(2) else List())
        val node2 = graph.getNodeById(2).get
        node2.inboundNodes.toList shouldEqual (if(inStored) List(1) else List())
        node2.outboundNodes.toList shouldEqual (if(notMutual) List() else List(1))

        // Test multi-edge
        graph.addEdgeAllowingDuplicates(1, 2)
        graph.edgeCount shouldEqual (if (dir != StoredGraphDir.Mutual) 2 else 4)
        node1.inboundNodes.toList shouldEqual (if(notMutual) List() else List(2, 2))
        node1.outboundNodes.toList shouldEqual (if(outStored) List(2, 2) else List())
        node2.inboundNodes.toList shouldEqual (if(inStored) List(1, 1) else List())
        node2.outboundNodes.toList shouldEqual (if(notMutual) List() else List(1, 1))

        graph.addEdge(2, 1)
        graph.edgeCount shouldEqual (if (dir != StoredGraphDir.Mutual) 3 else 4)
      }
    }

    "support deleting edges" in {
      for (dir <- graphDirections) {
        val graph = new ArrayBasedDynamicDirectedGraph(dir)
        val inStored = graph.isDirStored(GraphDir.InDir)
        val outStored = graph.isDirStored(GraphDir.OutDir)
        val notMutual = dir != StoredGraphDir.Mutual

        graph.addEdge(1, 2)
        graph.addEdge(1, 3)
        graph.removeEdge(1, 2)
        graph.edgeCount shouldEqual (if (notMutual) 1 else 2)
        graph.nodeCount shouldEqual 3 // This is debatable but reasonable.
        val node1 = graph.getNodeById(1).get
        node1.inboundNodes.toList shouldEqual (if(notMutual) List() else List(3))
        node1.outboundNodes.toList shouldEqual (if(outStored) List(3) else List())
        val node2 = graph.getNodeById(2).get
        node2.inboundNodes.toList shouldEqual List()
        node2.outboundNodes.toList shouldEqual List()
        val node3 = graph.getNodeById(3).get
        node3.inboundNodes.toList shouldEqual (if(inStored) List(1) else List())
        node3.outboundNodes.toList shouldEqual (if(notMutual) List() else List(1))
      }
    }
  }
}
