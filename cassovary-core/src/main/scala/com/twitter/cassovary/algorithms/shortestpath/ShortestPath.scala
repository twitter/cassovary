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

import com.twitter.cassovary.graph.{Node, DirectedGraph}

/**
 * A base trait for all ShortestPath algorithm implementations
 */
trait ShortestPath {
  type Path  = Seq[Int]

  val graph: DirectedGraph[Node]

  /**
   * Source node for all shortest paths calculated by `shortestPaths`
   */
  val source: Int

  /**
   * Calculate the shortest paths to `target` from a given source node.  The source
   * node must be provided as an instance variable of all implementing subclasses.
   * @param target The integer ID of the target node
   * @return A sequence of paths.  If no shortest path exists, the list will be empty
   */
  def shortestPaths(target: Int): Seq[Path]

  /**
   * Calculate all shortest paths for all target nodes in the graph
   * @return A map of integer node id to paths
   */
  def allShortestPaths: Map[Int, Seq[Path]] = graph.map { n => n.id -> shortestPaths(n.id) }.toMap
}
