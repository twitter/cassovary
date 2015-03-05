/*
 * Copyright 2015 Twitter, Inc.
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
package com.twitter.cassovary.algorithms.shortestpath

import com.twitter.cassovary.graph._

/**
 * Starting from a given source, calculate the shortest paths from that source to
 * a given target node.  This algorithm returns all shortest paths available not just
 * a single shortest path.
 * @param source The source node with which all shortest paths start.
 */
class SingleSourceShortestPath(val graph: DirectedGraph[Node], val source: Int) extends ShortestPath {
  private val spt = new ShortestPathsTraverser(graph, Seq(source))

  //We need to run the iterator...
  spt.toList

  def shortestPaths(target: Int): Seq[Path] = spt.paths(target)
}
