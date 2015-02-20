/*
 * Copyright 2014 Twitter, Inc.
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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.GraphDir._
import scala.util.Sorting
import scala.collection.mutable

/**
 * This class contains some common utilities and convenience functions for directed graphs.
 * It differs from {@code GraphUtils} because it works on a directed graph (which provides
 * some other things, such as an iterator on top of the underlying {@code Graph})
 */
class DirectedGraphUtils[+V <: Node](val directedGraph: DirectedGraph[V]) extends GraphUtils(directedGraph) {

  /** *
    * Returns the number of mutual edges in the graph (i.e., edges of type a->b and b->a both exist)
    * Warning: creates a copy of all edges, so it is memory inefficient. Better would be to spill
    * edges to disk intelligently and do an external sort.
    * @return number of mutual edges
    */
  def getNumMutualEdges: Long = {
    val edges = mutable.ArrayBuffer[Long]()
    if (directedGraph.isDirStored(InDir) && directedGraph.isDirStored(OutDir)) {
      val mutualEdges = directedGraph.foldLeft(0L) { (sum, node) => sum + getNumMutualEdgesBothDirs(node) }
      mutualEdges/2
    } else {
      val indir = directedGraph.isDirStored(InDir)
      directedGraph.foreach { node =>
        val neighbors = if (indir) node.inboundNodes else node.outboundNodes
        neighbors.foreach { destNodeId =>
          val sourceId = node.id.toLong
          val destId = destNodeId.toLong
          val edge = if (sourceId < destId) (sourceId << 32) | destId
          else (destId << 32) | sourceId
          edges += edge
        }
      }
      val edgesArr = edges.toArray
      Sorting.quickSort(edgesArr)
      val (numMutualEdges, _) = edgesArr.foldLeft((0L, -1L)) { case ((sum, prev), curr) =>
        val n = if (prev == curr) 1L else 0L
        (sum + n, curr)
      }
      numMutualEdges
    }
  }

}
