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
import com.twitter.cassovary.graph._
import com.twitter.cassovary.graph.tourist.PrevNbrCounter

class BetweenessCentrality(graph: DirectedGraph, normalize: Boolean = true) extends AbstractCentrality(graph) {

  private def combineMap [A, @specialized(Int, Double, Float, Long) B]
  (a: Map[A,B], b: Map[A,B])(implicit ev: B => Double) : Map[A,B] = {
    (a.toList ++ b.toList).groupBy(l => l._1).map{ t => t._1 -> t._2
      .foldLeft(0.0) { (x,y) => x + y._2 }.asInstanceOf[B] }
  }

  def _recalc(): Unit = {
    val tempCentralityValues = graph.foldLeft(new Array[Double](graph.maxNodeId + 1)) {
      (partialCentralityValues, node) =>
      val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(node.id), Walk.Limits(), Some(new PrevNbrCounter))
      val (nodes, depths) = bfs.foldLeft((List.empty[Node], Map.empty[Node, Int])) {
        case ((ns, d), n) =>
          (ns ++ List(n), d ++ Map(n -> bfs.depth(n.id).get))
      }
      val previous = nodes.map { n =>
        n -> n.inboundNodes().filter { in => depths(graph.getNodeById(in).get) == depths(n) - 1}
      }.toMap
      val sigma = Map(node -> 1) ++ nodes.foldLeft(Map.empty[Node, Int]) { (partialSigma, n) =>
        val nearestOut = n.outboundNodes filter { ou => depths(graph.getNodeById(ou).get) == depths(n) + 1}
        val currentSigma = nearestOut.map { o => graph.getNodeById(o).get -> partialSigma.getOrElse(n, 1)}.toMap
        val totalSigma = combineMap(partialSigma, currentSigma)
        totalSigma
      }
      val (currentCentrality, _) = nodes.reverse.foldLeft (
        (new Array[Double](graph.maxNodeId + 1), Map.empty[Int, Double])) {
        case ((partialCent, partialDelta), n) =>
          val coef = (1.0 + partialDelta.getOrElse(n.id, 0.0)) / sigma(n)
          val currentDelta = previous(n).map { prev => prev -> sigma(graph.getNodeById(prev).get) * coef}.toMap
          val totalDelta = combineMap(partialDelta, currentDelta)
          if (n.id != node.id)
            partialCent(n.id) += totalDelta.getOrElse(n.id, 0.0)
          (partialCent, totalDelta)
      }
      partialCentralityValues.zipWithIndex.map { case (v, i) => currentCentrality(i) + v}
    }
    val nc = graph.nodeCount
    val scale = if (normalize && nc >= 2) 1.0 / ((nc - 1) * (nc - 2)) else 1.0
    tempCentralityValues.zipWithIndex.foreach { case (v, i) => centralityValues(i) = v * scale }
  }
}
