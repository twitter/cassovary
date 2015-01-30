/*
* Copyright 2014 Twitter, Inc.
*
* Licensed under the Apache License, Version 2.0 (the "License"); you may not use this
* file except in compliance with the License. You may obtain a copy of the License at
*
* http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software distributed
* under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
* CONDITIONS OF ANY KIND, either express or implied. See the License for the
* specific language governing permissions and limitations under the License.
*/
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

  override val storedGraphDir: StoredGraphDir = StoredGraphDir.Mutual

  override def nodeCount: Int = outboundGraph.nodeCount
}

class UndirectedNode(outboundNode: Node) extends Node {
  override def outboundNodes(): Seq[Int] = outboundNode.outboundNodes()

  override def inboundNodes(): Seq[Int] = outboundNode.outboundNodes()

  override val id: Int = outboundNode.id
}
