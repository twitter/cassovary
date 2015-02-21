package com.twitter.cassovary.algorithms.shortestpath

import com.twitter.cassovary.graph.{GraphDir, BreadthFirstTraverser, Node, DirectedGraph}

/**
 * Created by bmckown on 2/20/15
 */
case class SingleSourceShortestPath(graph: DirectedGraph, source: Int) extends ShortestPath {

  val bfs: BreadthFirstTraverser = new BreadthFirstTraverser(graph, GraphDir.OutDir, Seq(source))
  private val (nodes, depths)  = (bfs.toList, bfs.allDepths)
  private val previous = nodes.map { n =>
    n.id -> n.inboundNodes().filter { in => depths(in) == depths(n.id) - 1}
  }.toMap

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
        currStack.zipWithIndex.map { case (seq, in) => if (in == currTop + 1) Seq(previous(node)(index), 0) else seq}
      (newStack, currTop + 1)
    }
    else {
      val newStack = currStack.zipWithIndex.map { case (seq, in) => if (in == currTop - 1) Seq(seq(0), seq(1) + 1) else seq}
      (newStack, currTop - 1)
    }
    (stack, top, path)
  }

  def shortestPaths(target: Int): Paths = {
    lazy val stream: Stream[(Stack, Int, Path)] =
      (List(Seq(target, 0)), 0, Seq.empty[Int]) #:: stream.map { case (s, t, p) => walkOnGraph(s,t) }

    stream.takeWhile{ case (_, t, _) => t >= 0}.toList
      .filter{ case (_, _, p) => p.nonEmpty }
      .map{ _._3 }.toSeq
  }
}
