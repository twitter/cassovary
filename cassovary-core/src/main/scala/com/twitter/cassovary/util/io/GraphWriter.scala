/*
 * Copyright 2013 Twitter, Inc.
 *
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
package com.twitter.cassovary.util.io

import com.twitter.cassovary.graph._
import java.io.{PrintWriter,Writer}

/**
 * Utility class for writing a graph object to a Writer output stream,
 * such that it could be read back in by a GraphReader.
 */
object GraphWriter {
  /**
   * Writes `node` to a given `writer`.
   */
  private def writeNode(writer: PrintWriter)(node: Node) {
    writer.println(node.id + " " + node.outboundCount)
    node.outboundNodes().foreach { ngh =>
      writer.println(ngh)
    }
  }

  /**
   * Writes given graph to given writer by iterating over all nodes and outbound edges.
   */
  def writeDirectedGraph[V <: Node](graph: DirectedGraph[V], writer: Writer): Unit = writeDirectedGraph(graph, Seq(writer))

  /**
   * Writes given graph to given writers dividing the graph into chunks
   * of equal sizes of nodes.
   */
  def writeDirectedGraph[V <: Node](graph: DirectedGraph[V], writers: Seq[Writer]): Unit = {
    val chunks = writers.size
    val nodesForChunk = ((graph.nodeCount - 1) / chunks) + 1
    graph.grouped(nodesForChunk).zip(writers.iterator).foreach {
      case (nodes, writer) =>
        val gWriter = new PrintWriter(writer)
        nodes foreach writeNode(gWriter)
        gWriter.close()
    }
  }

}
