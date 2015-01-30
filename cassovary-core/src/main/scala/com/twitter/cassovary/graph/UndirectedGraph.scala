package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir

/*
 * A graph where the outbound neighbors of each node equal the inbound neighbors.
 * Given a DirectedGraph which stores the neighbors of each node (as outbound edges),
 * this class wraps that graph in an undirected view.
 */

class UndirectedGraph(outboundGraph: DirectedGraph) extends DirectedGraph {
  override def getNodeById(id: Int): Option[Node] = outboundGraph.getNodeById(id) map { new UndirectedNode(_)}

  override def iterator: Iterator[Node] = outboundGraph.iterator map { new UndirectedNode(_)}

  override def edgeCount: Long = outboundGraph.edgeCount

  override val storedGraphDir: StoredGraphDir = outboundGraph.storedGraphDir

  override def nodeCount: Int = outboundGraph.nodeCount
}

class UndirectedNode(outboundNode: Node) extends Node {
  override def outboundNodes(): Seq[Int] = outboundNode.outboundNodes()

  override def inboundNodes(): Seq[Int] = outboundNode.outboundNodes()

  override val id: Int = outboundNode.id
}
