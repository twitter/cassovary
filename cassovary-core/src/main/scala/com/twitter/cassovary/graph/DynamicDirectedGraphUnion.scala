package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.graph.node.DynamicNode

/**
 * Wraps a directed graph and a dynamic directed graph to create a dynamic graph with the union of their nodes and edges.  When
 * edge are added, they are added to the underlying dynamic graph.  Node or edge deletion is not supported.
 */
class DynamicDirectedGraphUnion(staticGraph: DirectedGraph[Node], dynamicGraph: DynamicDirectedGraph[DynamicNode])
    extends DynamicDirectedGraph[DynamicNode] {
  // Because computing nodeCount as an intersection is expensive, maintain nodeCount as a variable.
  private var _nodeCount = if (dynamicGraph.nodeCount == 0)
      staticGraph.nodeCount
    else
    ((staticGraph map (_.id)).toSet ++ (dynamicGraph map (_.id)).toSet).size

  override def getNodeById(id: Int): Option[DynamicNode] =
    if (staticGraph.existsNodeId(id) || dynamicGraph.existsNodeId(id)) {
      Some(new DynamicNodeUnion(
        staticGraph.getNodeById(id),
        dynamicGraph.getOrCreateNode(id)))
    } else {
      None
    }

  override def getOrCreateNode(id: Int): DynamicNode = {
    if (!staticGraph.existsNodeId(id) && !dynamicGraph.existsNodeId(id)) {
      _nodeCount += 1
    }
    new DynamicNodeUnion(staticGraph.getNodeById(id), dynamicGraph.getOrCreateNode(id))
  }

  /**
   * Adds the given edge to the underlying dynamic graph. Note that for efficiency we don't check if the edge already exists,
   * so if the edge already exists, a 2nd copy of it will be added.
   */
  override def addEdge(srcId: Int, destId: Int): Unit = dynamicGraph.addEdge(srcId, destId)

  /** Not supported. */
  override def removeEdge(srcId: Int, destId: Int): (Option[Node], Option[Node]) = throw new UnsupportedOperationException()

  override def edgeCount: Long = staticGraph.edgeCount + dynamicGraph.edgeCount
  
  override def nodeCount: Int = _nodeCount

  assert(staticGraph.storedGraphDir == dynamicGraph.storedGraphDir)
  override val storedGraphDir: StoredGraphDir = dynamicGraph.storedGraphDir

  override def iterator: Iterator[DynamicNode] = {
    val staticGraphIds = staticGraph.iterator map (_.id)
    val additionalDynamicGraphIds = dynamicGraph.iterator map (_.id) filter (!staticGraph.existsNodeId(_))
    (staticGraphIds ++ additionalDynamicGraphIds) map (id => getNodeById(id).get)
  }
}

/** Represents the union of two nodes. */
private class DynamicNodeUnion(staticNodeOption: Option[Node],
                               dynamicNode: DynamicNode) extends DynamicNode {
  override val id: Int = dynamicNode.id

  override def inboundNodes(): Seq[Int] = staticNodeOption match {
    case Some(staticNode) => new IndexedSeqUnion(staticNode.inboundNodes(), dynamicNode.inboundNodes())
    case None => dynamicNode.inboundNodes()
  }
  override def outboundNodes(): Seq[Int] = staticNodeOption match {
    case Some(staticNode) => new IndexedSeqUnion(staticNode.outboundNodes(), dynamicNode.outboundNodes())
    case None => dynamicNode.outboundNodes()
  }

  // To make sure an edge (u, v) is added to both u's out-neighbors and v's in-neighbors,
  // mutations should happen through the graph.
  override def addInBoundNodes(nodeIds: Seq[Int]): Unit = throw new UnsupportedOperationException()
  override def addOutBoundNodes(nodeIds: Seq[Int]): Unit = throw new UnsupportedOperationException()
  override def removeInBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
  override def removeOutBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
}

/** Represents the concatanation of two IndexedSeqs. */
// TODO: We assume xs and ys have efficient random access (are effectively IndexedSeqs).  Refcatoring Node to return
// IndexedSeq would remove this assumption
private class IndexedSeqUnion[A](xs: Seq[A], ys: Seq[A]) extends IndexedSeq[A] {
  override def length: Int = xs.size + ys.size

  override def apply(i: Int): A =
    if (i < xs.size) {
      xs(i)
    } else if (i - xs.size < ys.size) {
      ys(i - xs.size)
    } else {
      throw new IndexOutOfBoundsException(s"Invalid index $i")
    }
}
