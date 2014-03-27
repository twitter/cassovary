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
package com.twitter.cassovary.algorithms

import com.twitter.cassovary.graph.DirectedGraph
import scala.util.Random
import scala.collection.mutable.{ArrayBuffer, BitSet}
import scala.collection.mutable

case class TriangleCountParameters(edgeReservoirSize : Int, wedgeReservoirSize : Int)

/**
 * Implementation of approximated triangle counting algorithm by Madhav Jha et al.
 * http://arxiv.org/pdf/1212.2264v3.pdf
 *
 * We assume that `graph` is undirected, formally if edge (a, b) is in
 * `graph` than (b, a) is also.
 */
object TriangleCount {
  def apply(graph: DirectedGraph, parameters: TriangleCountParameters): (Double, Double) = {
    new TriangleCount(graph, parameters).apply()
  }
}

/**
 * A wrapper of an array of tuples used to save memory.
 */
class IntTuplesArray(noOfTuples : Int, tupleSize : Int) extends Iterable[Seq[Int]] {
  /**
   * Tuples are stored in one table as consecutive elements.
   *
   * If tuple is equal to `(0, 0, ..., 0)` we assume it was not yet initialized.
   */
  var table = Array.fill[Int](tupleSize * noOfTuples)(0)

  def apply(pos : Int) : Seq[Int] = {
    require(pos < noOfTuples)
    table.slice(tupleSize * pos, tupleSize * pos + tupleSize)
  }

  def set(pos : Int, elems : Seq[Int]) {
    elems.zipWithIndex.foreach{
      case (elem, index) => table(tupleSize * pos + index) = elem
    }
  }

  def isTupleNonEmpty(seq : Seq[Int]) = !seq.forall(_ == 0)

  def tuplesIterator: Iterator[Seq[Int]] = new Iterator[Seq[Int]] {
    var cur = -1
    override def next(): Seq[Int] = if (hasNext) {cur += 1; apply(cur)} else Iterator.empty.next()

    override def hasNext: Boolean = cur < noOfTuples - 1
  }

  /**
   * Iterator over tuples stored in the table.
   */
  override def iterator: Iterator[Seq[Int]] = tuplesIterator.filter(isTupleNonEmpty)

  def distinctIterator = iterator.toSet.iterator

  override def mkString(sep: String) = {
    tuplesIterator.map(x => x.mkString(" ")).mkString(sep)
  }
}

class TriangleCount(graph : DirectedGraph, parameters : TriangleCountParameters) {

  val rng = new Random

  /**
   * We store an undirected edge as a list of ints `List(a, b)`, where `a < b`.
   */
  val edgeReservoir = new IntTuplesArray(parameters.edgeReservoirSize, 2)

  /**
   * A wedge is a list of ints List(a, b, c), where a < c and
   * (a, b) and (b, c) are undirected edges in the graph.
   */
  val wedgeReservoir = new IntTuplesArray(parameters.wedgeReservoirSize, 3)

  /**
   * `isClosed(i)` iff `wedgeReservoid(i)` is a closed wedge.
   */
  var isClosed = mutable.BitSet()

  var totalWedges = 0L

  def computeWedgesInEdgeReservoir(edgeReservoir: IntTuplesArray) : Int = {
    val nodesCounts = mutable.HashMap[Int, Int]()
    edgeReservoir.distinctIterator.foreach {
      case Seq(from, to) =>
        nodesCounts(from) = nodesCounts.getOrElse(from, 0) + 1
        nodesCounts(to) = nodesCounts.getOrElse(to, 0) + 1
    }
    nodesCounts.iterator.map{ case (k, v) => v * (v - 1) / 2}.sum
  }

  /**
   * Updates fields with edge `(fromNode, toNode)`. Assumes
   * that `fromNode < toNode`.
   * @param iteration Iteration number
   * @param fromNode Id of node with lower id
   * @param toNode Id of node with higer id
   * @return Pair: fraction of closed wedges in wedgeReservoir, total distinct wedges in edge reservoir.
   */
  def update(iteration : Int, fromNode : Int, toNode : Int) : (Double, Long) = {
    require(fromNode < toNode)
    (0 until parameters.wedgeReservoirSize).foreach {
      i =>
        val wedge = wedgeReservoir(i)
        if (wedge(0) == fromNode && wedge(2) == toNode) {
          isClosed(i) = true
        }
    }
    var updated = false
    (0 until parameters.edgeReservoirSize).foreach {
      i =>
        if (rng.nextDouble <= 1.0 / iteration) {
          edgeReservoir.set(i, List(fromNode, toNode))
          updated = true
        }
    }
    if (updated) {
      totalWedges = computeWedgesInEdgeReservoir(edgeReservoir)
      val newWedges = edgeReservoir.distinctIterator
        .filter(edge => edge.contains(fromNode) ^ edge.contains(toNode)).toArray
      if (newWedges.nonEmpty) {
        (0 until parameters.wedgeReservoirSize).foreach {
          wedgeNum =>
            if (rng.nextDouble() < newWedges.size.toDouble / totalWedges) {
              val randomNewWedge = createWedge(randomFromArray(newWedges), Seq(fromNode, toNode))
              wedgeReservoir.set(wedgeNum, randomNewWedge)
              isClosed(wedgeNum) = false
            }
        }
      }
    }
    (isClosed.size.toDouble / parameters.wedgeReservoirSize, totalWedges)
  }

  /**
   * Creates wedge from two edges. For each edge `(a, b)` we assume `a < b`.
   * @return A wedge `(a, b, c)` that fulfills `a < c`.
   */
  private def createWedge(edge1: Seq[Int], edge2: Seq[Int]) : Seq[Int] = {
    edge1 ++ edge2 match {
      case Seq(e1, e2, e3, e4) if e1 == e3 => e2.min(e4) +: e1 +: e2.max(e4) +: Seq()
      case Seq(e1, e2, e3, e4) if e2 == e3 && e1 < e4 => e1 +: e2 +: e4 +: Seq()
      case Seq(e1, e2, e3, e4) if e1 == e4 && e3 < e2 => e3 +: e1 +: e2 +: Seq()
      case Seq(e1, e2, e3, e4) if e2 == e4 => e1.min(e3) +: e2 +: e1.max(e3) +: Seq()
    }
  }

  private def randomFromArray[N](a : Array[N]) : N = {
    a(rng.nextInt(a.length))
  }

  def apply() : (Double, Double) = {
    var edgeNo = 1
    var transitivity = 0.0
    var triangles = 0.0
    graph.foreach {
      case fromNode => fromNode.outboundNodes().foreach {
        case toNode if toNode > fromNode.id =>
          val (pi, totalWedges) = update(edgeNo, fromNode.id, toNode)
          transitivity = 3.0 * pi
          triangles = (pi * edgeNo * edgeNo) / (parameters.edgeReservoirSize * (parameters.edgeReservoirSize - 1)) *
            totalWedges
          edgeNo += 1
        case _ => ()
      }
    }
    (transitivity, triangles)
  }
}