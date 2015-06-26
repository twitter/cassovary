/*
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

import com.twitter.cassovary.util.io.{IntLongSource, MemoryMappedIntLongSource}
import java.io._

/**
 * A graph which reads edge data from a memory mapped file.  There is no object overhead per
 * node: the memory used for n nodes and m edges with both in-neighbor and out-neighbor access is
 * exactly 16 + 16*n + 8*m bytes. Also, loading is very fast because no parsing of text is required.
 * Loading time is exactly the time it takes the operating system to map data from disk into
 * memory.  Nodes are numbered sequentially from 0 to nodeCount - 1 and must be a range of this
 * form (i.e. nodeCount == maxNodeId + 1).
 *
 * When transforming a graph where nodeCount <= maxNodeId
 * to this format, new nodes with no neighbors will be implicitly created.  Currently only supports
 * storing both in-neighbors and out-neighbors of nodes (StoredGraphDir.BothInOut). The binary
 * format is currently subject to change.  Node objects are created on demand when getNodeById is
 * called.
 */

/* Storage format
byteCount  data
8          (reserved, later use for versioning or indicating undirected vs directed)
8          n (i. e. the number of nodes).  Currently must be less than 2^31.
8*(n+1)    Offsets into out-neighbor data. Index i (a Long) points to the out-neighbor data of
           node i.  The out-neighbor data must be stored in sequential order by id, as the
           outegree of node i is computed from the difference in offset between node i+1 and node i.
           Index n is needed to compute the outdegree of node n - 1.
8*(n+1)    Offsets into in-neighbor data (Longs) (Same interpretation as out-neighbor offsets)
m          out-neighbor data
m          in-neighbor data
 */
class MemoryMappedDirectedGraph(file: File) extends DirectedGraph[Node] {
  val data: IntLongSource = new MemoryMappedIntLongSource(file)

  val nodeCount = data.getLong(8).toInt // In the future we may want to support Long ids, so
  // store nodeCount as Long

  private def outboundOffset(id: Int): Long = data.getLong(16L + 8L * id)

  private def outDegree(id: Int): Int = ((outboundOffset(id + 1) - outboundOffset(id)) / 4).toInt

  private def inboundOffset(id: Int): Long = data.getLong(16L + 8L * (nodeCount + 1) + 8L * id)

  private def inDegree(id: Int): Int = ((inboundOffset(id + 1) - inboundOffset(id)) / 4).toInt

  /* Only created when needed (there is no array of these stored). */
  private class MemoryMappedDirectedNode(override val id: Int) extends Node {
    val nodeOutboundOffset = outboundOffset(id)
    val nodeInboundOffset = inboundOffset(id)
    def outboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = outDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeOutboundOffset + 4L * i)
    }
    def inboundNodes(): Seq[Int] = new IndexedSeq[Int] {
      val length: Int = inDegree(id)
      def apply(i: Int): Int =  data.getInt(nodeInboundOffset + 4L * i)
    }
  }

  def getNodeById(id: Int): Option[Node] =
    if(0 <= id && id < nodeCount) {
      Some(new MemoryMappedDirectedNode(id))
    } else {
      None
    }

  def iterator: Iterator[Node] = (0 to nodeCount).iterator flatMap (i => getNodeById(i))

  lazy val edgeCount: Long = outboundOffset(nodeCount) - outboundOffset(0)

  override lazy val maxNodeId = nodeCount - 1

  val storedGraphDir = StoredGraphDir.BothInOut
}

object MemoryMappedDirectedGraph {
  /** Writes the given graph to the given file (overwriting it if it exists) in the current binary
   * format.
   */
  def graphToFile(graph: DirectedGraph[Node], file: File): Unit = {
    val n = graph.maxNodeId + 1 // includes both 0 and maxNodeId as ids
    val out = new DataOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    out.writeLong(0)
    out.writeLong(n)
    //The outneighbor data starts after the initial 8 bytes, n+1 Longs for outneighbors, and n+1
    // Longs for in-neighbors
    var outboundOffset = 16L + 8L * (n + 1) * 2
    for (i <- 0 until n) {
      out.writeLong(outboundOffset)
      outboundOffset += 4 * (graph.getNodeById(i) map (_.outboundCount)).getOrElse(0)
    }
    out.writeLong(outboundOffset) // Needed to compute outdegree of node n-1

    // The inbound data starts immediately after the outbound data
    var inboundOffset = outboundOffset
    for (i <- 0 until n) {
      out.writeLong(inboundOffset)
      inboundOffset += 4 * (graph.getNodeById(i) map (_.inboundCount)).getOrElse(0)
    }
    out.writeLong(inboundOffset) // Needed to compute indegree of node n-1

    for (i <- 0 until n) {
      for (v <- (graph.getNodeById(i) map (_.outboundNodes())).getOrElse(Nil)) {
        out.writeInt(v)
      }
    }
    for (i <- 0 until n) {
      for (v <- (graph.getNodeById(i) map (_.inboundNodes())).getOrElse(Nil)) {
        out.writeInt(v)
      }
    }
    out.close()
  }
}
