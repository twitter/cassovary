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

  val bfs = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(source))
  private val (nodes, depths)  = (bfs.toList, bfs.allDepths)
  private val previous = nodes.map { n =>
    n.id -> n.inboundNodes().filter { in => depths(in) == depths(n.id) - 1}
  }.toMap

  /**
   * Talk a walk on our graph starting at a given node.  Once the source node has been reached,
   * the walk continues by retracing its steps.  At each step backward, the algorithm asks if there
   * exist any other nodes that could lead to a shortest path.  If so, walk along those nodes, else
   * continue to walk backward.  The algorithm terminates when it has reached the target node and
   * has no other place to go but to retrace its steps (which is impossible).
   * @param currStack The current path of nodes that the algorithm is following.
   * @param currTop The number of steps the algorithm has taken from target toward source.
   * @return A Tuple3 consisting of a new stack, a new top, and a path.  The path will be
   *         empty if a shortest path was not successfully found during that iteration.
   */
  private def walkOnGraph(currStack: Stack, currTop: Int): (Stack, Int, Path) = {
    val node  = currStack(currTop)(0)
    val index = currStack(currTop)(1)

    val path = if (node == source)
      currStack.take(currTop + 1).reverse.map { k => k(0) }.toSeq
    else
      Seq.empty[Int]

    val (stack, top) = if (previous(node).length > index) {
      val newStack = if (currTop + 1 == currStack.length)
        currStack :+ Seq(previous(node)(index), 0)
      else
        currStack.zipWithIndex.map { case (seq, in) =>
          if (in == currTop + 1) Seq(previous(node)(index), 0) else seq }
      (newStack, currTop + 1)
    }
    else {
      val newStack = currStack.zipWithIndex.map { case (seq, in) =>
        if (in == currTop - 1) Seq(seq(0), seq(1) + 1) else seq }
      (newStack, currTop - 1)
    }
    (stack, top, path)
  }

  def shortestPaths(target: Int): Seq[Path] = {
    lazy val stream: Stream[(Stack, Int, Path)] =
      (List(Seq(target, 0)), 0, Seq.empty[Int]) #:: stream.map { case (s, t, p) => walkOnGraph(s,t) }

    stream.takeWhile{ case (_, t, _) => t >= 0 }.toList
      .filter{ case (_, _, p) => p.nonEmpty }
      .map{ _._3 }.toSeq
  }
}
