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

  /** Represents the union of two nodes. */
  // Inner class so it can access the addEdge method
  private class DynamicNodeUnion(staticNodeOption: Option[Node],
                                  dynamicNode: DynamicNode) extends DynamicNode {
    override val id: Int = dynamicNode.id

    override def inboundNodes(): Seq[Int] =
      (staticNodeOption map (_.inboundNodes())).getOrElse(Nil) ++ dynamicNode.inboundNodes()
    override def outboundNodes(): Seq[Int] =
      (staticNodeOption map (_.outboundNodes())).getOrElse(Nil) ++ dynamicNode.outboundNodes()

    override def addInBoundNodes(nodeIds: Seq[Int]): Unit = nodeIds map (addEdge(_, id))
    override def addOutBoundNodes(nodeIds: Seq[Int]): Unit = nodeIds map (addEdge(id, _))
    override def removeInBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
    override def removeOutBoundNode(nodeId: Int): Unit = throw new UnsupportedOperationException()
  }

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
