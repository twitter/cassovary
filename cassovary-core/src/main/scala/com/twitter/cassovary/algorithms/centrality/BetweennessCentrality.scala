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
package com.twitter.cassovary.algorithms.centrality

import com.twitter.cassovary.algorithms.shortestpath.SingleSourceShortestPath
import com.twitter.cassovary.graph._

class BetweennessCentrality(graph: DirectedGraph[Node], normalize: Boolean = true) extends AbstractCentrality(graph) {

  private def combineMap(a: Map[Int, Double], b: Map[Int, Double]): Map[Int, Double] = {
    (a.keys ++ b.keys).map { k => k -> (a.getOrElse(k, 0.0) + b.getOrElse(k, 0.0)) }.toMap
  }

  def _recalc(): Unit = {
    val values = graph.foldLeft(Map.empty[Int, Double]){ (partialValues, n) =>
      val allPaths = SingleSourceShortestPath(graph, n.id).allShortestPaths
      val currentValues = allPaths.values.foldLeft(Map.empty[Int, Double]){ (acc, pc) =>
        val middleNodes = pc.flatMap { p => p.slice(1, p.size - 1) }
          .groupBy(k => k)
          .map { k => k._1 -> (k._2.size.toDouble / pc.length) }.toMap
        combineMap(middleNodes, acc)
      }
      combineMap(partialValues, currentValues)
    }
    val nc = graph.nodeCount
    val scale = if (normalize && nc >= 2) 1.0 / ((nc - 1) * (nc - 2)) else 1.0
    values.foreach { case (i, v) => centralityValues(i) = v * scale }
  }
}
