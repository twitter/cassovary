package com.twitter.cassovary.graph

import org.scalatest.WordSpec
import com.twitter.cassovary.graph.StoredGraphDir._
import org.scalatest.matchers.ShouldMatchers

class ArrayBasedDynamicDirectedGraphSpec extends WordSpec with ShouldMatchers with GraphBehaviours {
  def builder(iteratorFunc: () => Iterator[NodeIdEdgesMaxId],
              storedGraphDir: StoredGraphDir) =
    new ArrayBasedDynamicDirectedGraph(iteratorFunc(), storedGraphDir)
  verifyGraphBuilding(builder, sampleGraphEdges)

  "Add new nodes" in {
    val graph = new ArrayBasedDynamicDirectedGraph(StoredGraphDir.BothInOut)
    for (i <- 0 until 3) {
      graph.nodeCount shouldEqual i
      graph.edgeCount shouldEqual 0
      graph.getOrCreateNode(10 * i) // non-contiguous
    }
    graph.getOrCreateNode(10) // Accessing again should increase node count
    graph.nodeCount shouldEqual 3
  }


  "Add new edges" in {
    for (dir <- List(StoredGraphDir.OnlyIn, StoredGraphDir.OnlyOut, StoredGraphDir.BothInOut)) {
      val graph = new ArrayBasedDynamicDirectedGraph(dir)
      val inStored = graph.isDirStored(GraphDir.InDir)
      val outStored = graph.isDirStored(GraphDir.OutDir)

      graph.addEdge(1, 2)
      graph.addEdge(1, 2) // Test duplicate elimination
      graph.edgeCount shouldEqual 1
      val node1 = graph.getNodeById(1).get
      node1.inboundNodes.toList shouldEqual List()
      node1.outboundNodes.toList shouldEqual (if(outStored) List(2) else List())
      val node2 = graph.getNodeById(2).get
      node2.inboundNodes.toList shouldEqual (if(inStored) List(1) else List())
      node2.outboundNodes.toList shouldEqual List()

      // Test multi-edge
      graph.addEdgeAllowingDuplicates(1, 2)
      graph.edgeCount shouldEqual 2
      node1.inboundNodes.toList shouldEqual List()
      node1.outboundNodes.toList shouldEqual (if(outStored) List(2, 2) else List())
      node2.inboundNodes.toList shouldEqual (if(inStored) List(1, 1) else List())
      node2.outboundNodes.toList shouldEqual List()

      graph.addEdge(2, 1)
      graph.edgeCount shouldEqual 3
    }
  }

  "Delete edges" in {
    for (dir <- List(StoredGraphDir.OnlyIn, StoredGraphDir.OnlyOut, StoredGraphDir.BothInOut)) {
      val graph = new ArrayBasedDynamicDirectedGraph(dir)
      val inStored = graph.isDirStored(GraphDir.InDir)
      val outStored = graph.isDirStored(GraphDir.OutDir)

      graph.addEdge(1, 2)
      graph.addEdge(1, 3)
      graph.removeEdge(1, 2)
      graph.edgeCount shouldEqual 1
      graph.nodeCount shouldEqual 3 // This is debatable but reasonable.
      val node1 = graph.getNodeById(1).get
      node1.inboundNodes.toList shouldEqual List()
      node1.outboundNodes.toList shouldEqual (if(outStored) List(3) else List())
      val node2 = graph.getNodeById(2).get
      node2.inboundNodes.toList shouldEqual List()
      node2.outboundNodes.toList shouldEqual List()
      val node3 = graph.getNodeById(3).get
      node3.inboundNodes.toList shouldEqual (if(inStored) List(1) else List())
      node3.outboundNodes.toList shouldEqual List()
    }
  }
}
