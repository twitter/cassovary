/*
 * Copyright 2012 Twitter, Inc.
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
package com.twitter.cassovary.graph.bipartite

import com.twitter.cassovary.graph.util.SmallBoundedPriorityQueue
import com.twitter.cassovary.graph.{GraphUtils, Node}
import com.twitter.ostrich.stats.Stats
import net.lag.logging.Logger
import scala.collection.mutable

/**
 * The node information supplied to the analyzer. With each node is supplied
 * {@code initialIterationWeight},
 * which is used to initialize the scores of every node at the beginning of every iteration.
 */
case class SuppliedNodeInfo(val node: Node, val initialIterationWeight: Double)

/**
 * Iteratively analyze links in a bipartite graph.
 * @param graphUtils GraphUtils encapsulating the underlying graph
 * @param resetProbLeft reset probability when iterating from left to right
 * @param resetProbRight reset probability when iterating from right to left
 * @param numTopContributors The number of contributors (reasons) to be kept for each score
 */
class IterativeLinkAnalyzer(graphUtils: GraphUtils, resetProbOnLeft: Double,
    resetProbOnRight: Double, numTopContributors: Int) {

  private val graph = graphUtils.graph
  private val log = Logger.get

  // keep track of associated info for every node while doing iterations
  case class NodeInfo(val node: Node, val initialIterationWeight: Double, var weight: Double,
      var numNeighbors: Int) extends Ordered[NodeInfo] {
    def compare(that: NodeInfo) = this.weight.compare(that.weight)
    val contributors = new SmallBoundedPriorityQueue[NodeInfo](numTopContributors)
    def shortString = "(id = %d , weight = %.2f , neighbors = %d , contributors = %s)".format(
      node.id, weight, numNeighbors, contributors.top(numTopContributors)
      )
    def topContributorIds = contributors.top(numTopContributors).map { _.node.id }
  }

  // do one iteration. Only maintain contributors in last iteration
  private def iterate(nodeInfos: Seq[mutable.HashMap[Int, NodeInfo]],
                      resetProb: Double, neighborsProvider: Node => Seq[Int],
                      flowReverse: Boolean,
                      isRightUninitialized: Boolean, isLastIter: Boolean) {

    def flowWeight(source: NodeInfo, dest: NodeInfo) {
      dest.weight += (source.weight / source.numNeighbors) * (1 - resetProb)
      if (isLastIter) dest.contributors += source
    }

    // flow weights from sourceInfos to destInfos
    val left = nodeInfos(0)
    val right = nodeInfos(1)

    var (sourceInfos, destInfos) = if (flowReverse) (right, left) else (left, right)

    // initialize dest
    destInfos.values foreach { destInfo =>
      destInfo.weight = resetProb * destInfo.initialIterationWeight
      if (isLastIter) destInfo.contributors.clear()
    }

    // actual iteration: always go from left to right
    left.values foreach { leftInfo =>
      neighborsProvider(leftInfo.node) foreach { rightNode =>
      // create rightInfo, calculate number of neighbors of rightNode in this subgraph
        val rightInfo = right.getOrElseUpdate(rightNode, {
          assert(isRightUninitialized)
          NodeInfo(graphUtils.graph.getNodeById(rightNode).get, 0.0, 0.0, 0)
        })
        if (isRightUninitialized) { rightInfo.numNeighbors += 1 }
        if (flowReverse)
          flowWeight(rightInfo, leftInfo)
        else
          flowWeight(leftInfo, rightInfo)
      }
    }
  }

  //TODO get a better return value type here!

  /**
   * Carries out link analysis on a bipartite graph, by default sort its output
   * @param leftNodes: Sequence of info about the nodes on the left side of the bipartite graph
   * @param numIterations: Number of iterations. A value of two means one pass from left to right
   * and then one from right to left.
   * @param neighborsProvider A function providing the neighbors of a node on the left
   */
  def analyze(leftNodes: Seq[SuppliedNodeInfo], numIterations: Int,
      neighborsProvider: Node => Seq[Int]):
      (List[(Int, Double, Seq[Int])], List[(Int, Double, Seq[Int])]) = {

    analyze(leftNodes, numIterations, neighborsProvider, true)

  }

  /**
   * Carries out link analysis on a bipartite graph.
   * @param leftNodes: Sequence of info about the nodes on the left side of the bipartite graph
   * @param numIterations: Number of iterations. A value of two means one pass from left to right
   * and then one from right to left.
   * @param neighborsProvider A function providing the neighbors of a node on the left
   * @param sortOutputByScore A boolean variable indicating if the output of the analysis
   * should be sorted
   */
  def analyze(leftNodes: Seq[SuppliedNodeInfo], numIterations: Int,
      neighborsProvider: Node => Seq[Int], sortOutputByScore: Boolean):
      (List[(Int, Double, Seq[Int])], List[(Int, Double, Seq[Int])]) = {

    def debugString(hash: collection.Map[Int, NodeInfo]) = {
      (hash.toList.sortBy(_._1).map { case (id, nodeInfo) =>
        "%d %.3f %s".format(id, nodeInfo.weight, nodeInfo.contributors.top(3).map{_.node.id})
      }).mkString("\n")
    }

    def listFrom(hash: collection.Map[Int, NodeInfo]) = {
      hash.valuesIterator.toList.map {
        nodeInfo => (nodeInfo.node.id, nodeInfo.weight, nodeInfo.topContributorIds)
      }
    }

    def sortedListFrom(hash: collection.Map[Int, NodeInfo]) = {
      listFrom(hash).sortBy( - _._2)
    }

    // TODO -- perhaps use a Pair as well as make it clearer

    // set up nodeInfos in both directions
    // one map in each direction, of which ids are present and their info
    val nodeInfos = List(new mutable.HashMap[Int, NodeInfo], new mutable.HashMap[Int, NodeInfo])
    nodeInfos(0) ++= leftNodes map { suppliedNodeInfo: SuppliedNodeInfo =>
      suppliedNodeInfo.node.id -> NodeInfo(suppliedNodeInfo.node,
        suppliedNodeInfo.initialIterationWeight, suppliedNodeInfo.initialIterationWeight,
        neighborsProvider(suppliedNodeInfo.node).length)
    }

    // iterate
    (1 to numIterations) foreach { iter =>
      val (flowReverse, resetProbUse) = if (iter % 2 == 1)
        (false, resetProbOnRight)
      else
        (true, resetProbOnLeft)
      val isRightUninitialized = (iter == 1)
      val isLastIterOnEitherSide = (iter == numIterations - 1 ) || (iter == numIterations)
      iterate(nodeInfos, resetProbUse, neighborsProvider, flowReverse,
          isRightUninitialized, isLastIterOnEitherSide)
      log.ifTrace { "After iteration %d , weights changed for %s side : \n%s".format(
        iter, if (flowReverse) "left" else "right",
        debugString(nodeInfos(iter % 2))) }
      log.ifDebug { "Finished iteration %d".format(iter) }
    }

    log.ifDebug {
      "Finished all iterations: LHS size = %d RHS size = %d LHS->RHS # = %d edges ".format(
      nodeInfos(0).size, nodeInfos(1).size, nodeInfos(0).values.foldLeft(0) { _ + _.numNeighbors}
    )}

    if (sortOutputByScore) {
      //sort output
      val sortedLeft = Stats.time("bila_sort_left") { sortedListFrom(nodeInfos(0)) }
      log.ifDebug { "Finished sorting left" }
      val sortedRight = Stats.time("bila_sort_right") { sortedListFrom(nodeInfos(1)) }
      log.ifDebug { "Finished sorting right" }
      (sortedLeft, sortedRight)
    } else {
      (listFrom(nodeInfos(0)), listFrom(nodeInfos(1)))
    }
  }

}
