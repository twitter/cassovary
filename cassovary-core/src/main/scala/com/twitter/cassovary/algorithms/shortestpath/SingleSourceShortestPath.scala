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
class SingleSourceShortestPath(val graph: DirectedGraph[Node], val source: Int) {
  type Path = Seq[Int]

  private val dir = graph.storedGraphDir match {
    case StoredGraphDir.OnlyIn => GraphDir.InDir
    case _ => GraphDir.OutDir
  }

  private lazy val sp = new ShortestPathsTraverser(graph, Seq(source), dir)
  private var completedWalk = false

  val ancestors = {
    if (!completedWalk) {
      sp.toList
      completedWalk = true
    }
    sp.ancestors
  }

  /**
   * Calculate all shortest paths for all target nodes in the graph
   * @return A map of integer node id to paths
   */
  lazy val allShortestPaths: Map[Int, Seq[Path]] = graph.par.map { n =>
    n.id -> findPaths(n.id).map {_.reverse } }.seq.toMap

  def shortestPaths(target: Int): Seq[Path] = allShortestPaths(target)
  def numPaths(target: Int): Int = allShortestPaths(target).size

  private def findPaths(start: Int, paths: Seq[Path] = Seq.empty[Path]): Seq[Path] = {
    val nextNodes = ancestors(start)
    if (start == source || nextNodes.isEmpty)
      paths.par map { _ :+ source } seq
    else {
      val update = if (paths.isEmpty) paths :+ Seq(start) else paths.par.map { _ :+ start }.seq
      (update ++ (nextNodes.par flatMap { n => findPaths(n, update) })).par filter { _.last == source } seq
    }
  }
}
