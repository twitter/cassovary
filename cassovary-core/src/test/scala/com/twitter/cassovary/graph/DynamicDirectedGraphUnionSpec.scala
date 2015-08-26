package com.twitter.cassovary.graph

import org.scalatest.{Matchers, WordSpec}

class DynamicDirectedGraphUnionSpec extends WordSpec with Matchers {
  val staticGraph1 = ArrayBasedDirectedGraph.apply(
    Iterable(
      NodeIdEdgesMaxId(1, Array(2, 3)),
      NodeIdEdgesMaxId(3, Array(1, 2)),
      NodeIdEdgesMaxId(5, Array(1))
    ),
    StoredGraphDir.BothInOut,
    NeighborsSortingStrategy.LeaveUnsorted)

  "A DynamicDirectedGraph" should {
    " correctly combine two graphs" in {
      staticGraph1.nodeCount shouldEqual (4)
      staticGraph1.edgeCount shouldEqual(5)
      val dynamicGraph = new SynchronizedDynamicGraph()
      dynamicGraph.addEdge(5, 6)
      val unionGraph = new DynamicDirectedGraphUnion(staticGraph1, dynamicGraph)
      (unionGraph map (_.id)) should contain theSameElementsAs (Seq(1, 2, 3, 5, 6))
      unionGraph.nodeCount shouldEqual (5)
      unionGraph.edgeCount shouldEqual(6)
      unionGraph.getNodeById(5).get.outboundNodes should contain theSameElementsAs (Seq(1, 6))
      unionGraph.getNodeById(6).get.inboundNodes should contain theSameElementsAs (Seq(5))
      unionGraph.getNodeById(5).get.outboundNodes should contain theSameElementsAs (Seq(1, 6))
      // Make sure getNodeById doesn't create the node
      unionGraph.getNodeById(4) should be (None)
      unionGraph.nodeCount shouldEqual (5)
      unionGraph.getOrCreateNode(4)
      unionGraph.nodeCount shouldEqual (6)
      unionGraph.addEdge(1, 4)
      unionGraph.getNodeById(4).get.inboundNodes should contain theSameElementsAs (Seq(1))
      unionGraph.addEdge(1, 6)
      unionGraph.getOrCreateNode(1).outboundNodes should contain theSameElementsAs (Seq(2, 3, 4, 6))
      unionGraph.edgeCount shouldEqual(8)
    }

    " throw an error given an invalid filename" in {
      a[NullPointerException] should be thrownBy {
        new DynamicDirectedGraphUnion(staticGraph1, null)
      }
    }
  }
}