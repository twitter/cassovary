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

import com.twitter.cassovary.graph.{Node, DirectedGraph}
import scala.collection.mutable

class BetweenessCentrality(graph: DirectedGraph, normalize: Boolean = true) extends AbstractCentrality(graph) {
def _recalc(): Unit = {}
//
//
//  private case class ShortestPath()
//  private def singleSourceShortestPath(g: DirectedGraph, s: Node): ShortestPath = {
//    val S = mutable.ListBuffer.empty[Node]
//    val paths = mutable.Map.empty[Node, mutable.Seq[Int]]
//    val sigma = mutable.Map.empty[Node, Double]
//    val D = mutable.Map.empty[Node, Double]
//
//    sigma(s) = 1.0
//    D(s) = 0
//    val Q = List(s)
//    while (Q.nonEmpty) {
//      val v = Q.head
//      S.append(v)
//      val Dv = D(v)
//      val sigmav = sigma(v)
//    }}
}
