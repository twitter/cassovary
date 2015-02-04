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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.GraphDir._
import com.twitter.cassovary.util.FastUtilUtils
import com.twitter.util.Duration
import com.twitter.util.Stopwatch
import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntMap
import org.scalatest.{Matchers, WordSpec}

// TODO add a fake random so that the random walk tests can be controlled
class GraphUtilsSpec extends WordSpec with Matchers {

  def utils[V <: Node](graph: DirectedGraph[V]) = {
    (graph, new GraphUtils(graph))
  }

  "two node graph with each following the other" should {
    val (graph, graphUtils) = utils(TestGraphs.g2_mutual)
    "have neighborCount always equal 1" in {
      graph.iterator foreach { node =>
        GraphDir.values foreach { dir =>
          graphUtils.neighborCount(node.id, dir) shouldEqual 1
        }
      }
    }

    "perform random walk with resetProb of 0" when {
      "1 step taken" in {
        // with reset prob of 0 and 1 step forward, it should always
        val resetProb = 0.0
        val numWalkSteps = 2L
        val walkParams = GraphUtils.RandomWalkParams(
          numWalkSteps, resetProb, None, Some(10), None, false, GraphDir.OutDir, false)

        val (visitsCounter, pathsCounterOption) = graphUtils.randomWalk(OutDir, Seq(1), walkParams)
        val visitsCountMap = visitsCounter.infoAllNodes
        visitsCountMap.toSeq shouldEqual Array((1, 1), (2, 1)).toSeq

        val pathsCountMap = pathsCounterOption.get.infoAllNodes
        pathMapToSeq(pathsCountMap(1)) shouldEqual Seq((DirectedPath(Array(1)), 1)).toSeq
        pathMapToSeq(pathsCountMap(2)) shouldEqual Seq((DirectedPath(Array(1, 2)), 1)).toSeq

        // random walk but no top paths maintained
        val (visitsCounter2, pathsCounterOption2) = graphUtils.randomWalk(OutDir, Seq(1),
          GraphUtils.RandomWalkParams(numWalkSteps, resetProb,
            None, None, None, false, GraphDir.OutDir, false))
        val visitCounterMap2 = visitsCounter2.infoAllNodes
        visitCounterMap2.toSeq shouldEqual Array((1, 1), (2, 1)).toSeq

        pathsCounterOption2.isDefined shouldEqual false
      }

      "more steps taken" in {
        val resetProb = 0.0
        var numTimesTested = 0
        for (numWalkSteps <- 2 to 8) {
          for (startNodeId <- 1 to 2) {
            numTimesTested += 1
            val othernd = if (startNodeId == 1) 2 else 1
            val walkParams = GraphUtils.RandomWalkParams(
              numWalkSteps, resetProb, None, Some(10), None, false, GraphDir.OutDir, false)

            val (visitsCounter, pathsCounterOption) = graphUtils.randomWalk(OutDir, Seq(startNodeId), walkParams)
            visitsCounter.infoAllNodes(startNodeId) shouldEqual (numWalkSteps / 2 + (numWalkSteps % 2))
            visitsCounter.infoAllNodes(othernd) shouldEqual numWalkSteps / 2
            pathsCounterOption.get.infoAllNodes.size shouldEqual 2
          }
        }
        numTimesTested shouldEqual (7 * 2)
      }
    }
  }

  "three node graph utils" should {
    val (_, graphUtils) = utils(TestGraphs.g3)
    "perform bfs walk with correct depths of visiting" in {
      val visitedNodes = graphUtils.bfsWalk(10, GraphDir.OutDir,
        Walk.Limits(Some(5), None, Some(10)))
      visitedNodes shouldEqual Array((10, 0), (11, 1), (12, 1)).toSeq
    }
  }

  "six node graph utils" should {
    val (graph, graphUtils) = utils(TestGraphs.g6)

    "pass basic graph checks" in {
      graph.nodeCount shouldEqual 6
      graph.edgeCount shouldEqual 11
      graphUtils.neighborCount(10, GraphDir.OutDir) shouldEqual 3
      graphUtils.neighborCount(11, GraphDir.InDir) shouldEqual 2
    }

    "have correct distribution of nodes visits in a random walk of 1000 steps" in {
      val resetProb = 0.2
      val numWalkSteps = 1000L
      val startNodeId = 10
      val walkParams = GraphUtils.RandomWalkParams(numWalkSteps, resetProb, None,
        Some(numWalkSteps.toInt), None, false, GraphDir.OutDir, false)

      val (visitsCounter, pathsCounterOption) = graphUtils.randomWalk(OutDir, Seq(startNodeId), walkParams)
      // expect to have every node visited a few times
      val minNumVisitsExpected = numWalkSteps.toInt / 100

      val nodeIterator = visitsCounter.infoAllNodes.keySet.iterator
      while (nodeIterator.hasNext) {
        val node = nodeIterator.next()
        (visitsCounter.infoAllNodes(node) >= minNumVisitsExpected) shouldEqual true
      }
    }

    "compute correct personalized reputation" in {
      val walkParams = GraphUtils.RandomWalkParams(
          10000L, 0.5, None, Some(2), None, false, GraphDir.OutDir, false)
      val visitsPerNode = graphUtils.calculatePersonalizedReputation(10, walkParams)._1

      // 10000 steps should visit every node at least once
      visitsPerNode.size shouldEqual graph.nodeCount
      val nodeIterator = visitsPerNode.keySet.iterator
      while (nodeIterator.hasNext) {
        val node = nodeIterator.next()
        (visitsPerNode(node) >= 1) shouldEqual true
      }
      // TODO more testing here
    }

    "stabilize random walk correctly" in {
      val walkParams = GraphUtils.RandomWalkParams(
        10000L, 0.5, None, Some(2), None, false, GraphDir.OutDir, true)
      val visitsPerNode = graphUtils.calculatePersonalizedReputation(10, walkParams)._1
      val visitsPerNode2 = graphUtils.calculatePersonalizedReputation(10, walkParams)._1
      checkMapApproximatelyEquals(visitsPerNode, visitsPerNode2, 200) // Prob(fail) ~ 10^-7
    }

    "have maxDepth working properly (for random walk and reputation calculation)" in {
      val walkParams = GraphUtils.RandomWalkParams(7L, 0.0, None, Some(2), Some(1),
          false, GraphDir.OutDir, false)
      val visitsPerNode = graphUtils.calculatePersonalizedReputation(12, walkParams)._1
      visitsPerNode.size shouldEqual 1
      (visitsPerNode(12) >= 1) shouldEqual true
      val walkParams2 = GraphUtils.RandomWalkParams(8L, 0.0, None, Some(2), Some(2), false,
        GraphDir.OutDir, false)
      val visitsPerNode2 = graphUtils.calculatePersonalizedReputation(12, walkParams2)._1
      visitsPerNode2.size shouldEqual 2
      (visitsPerNode2(12) >= 1) shouldEqual true
      (visitsPerNode2(14) >= 1) shouldEqual true
    }

    "perform bfs walk with correct depths of visiting" in {
      val visitedNodes = graphUtils.bfsWalk(15, GraphDir.OutDir, Walk.Limits(Some(5), None, Some(10)))

      visitedNodes shouldEqual Array((15, 0), (10, 1), (11, 1), (12, 2), (13, 2), (14, 2)).toSeq
    }
  }

  "random walk on a large graph" should {
    val graph = TestGraphs.generateRandomGraph(10 * 1000, 0.01)
    val graphUtils = new GraphUtils(graph)

    "visit at least 10 nodes" in {
      val resetProb = 0.2
      val numWalkSteps = 1000L
      val numTimes = 3
      val ignoreFirstNum = 0
      val startNodeId = graph.iterator.next.id
      var sumDuration = 0L
      val walkParams = GraphUtils.RandomWalkParams(numWalkSteps, resetProb, None,
        Some(numWalkSteps.toInt), None, false, GraphDir.OutDir, false)
      (1 to (numTimes + ignoreFirstNum)) foreach { times =>
        val elapsed: () => Duration = Stopwatch.start()
        val walk = graphUtils.randomWalk(OutDir, Seq(startNodeId), walkParams)
        val duration: Duration = elapsed()
        val (visitsCounter, _) = walk
        visitsCounter.infoAllNodes.size should be > graph.getNodeById(startNodeId).get.outboundCount
        if (times > ignoreFirstNum) {
          sumDuration += duration.inMilliseconds
        }
      }
      //println("Avg duration over %d random walks: %s ms".format(numTimes, sumDuration/numTimes))
    }
  }

  def pathMapToSeq(map: Object2IntMap[DirectedPath]) = {
    FastUtilUtils.object2IntMapToArray(map).toSeq
  }

  def visitMapToSeq(map: Int2IntMap) = {
    FastUtilUtils.int2IntMapToArray(map).toSeq
  }

  def checkMapApproximatelyEquals(visitsPerNode: collection.Map[Int, Int], visitsPerNode2: collection.Map[Int, Int],
                                  delta: Int) {
    visitsPerNode.size shouldEqual visitsPerNode2.size

    val nodeIterator = visitsPerNode.keySet.iterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.next()
      (visitsPerNode(node) - visitsPerNode2(node) < delta) shouldEqual true
    }
  }
}
