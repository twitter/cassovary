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
package com.twitter.cassovary.graph

import com.twitter.cassovary.graph.GraphDir._
import com.twitter.util.Duration
import it.unimi.dsi.fastutil.ints.Int2IntMap
import org.specs.Specification

// TODO add a fake random so that the random walk tests can be controlled
class GraphUtilsSpec extends Specification {

  var graph: DirectedGraph = _
  var graphUtils: GraphUtils = _

  val twoNodeGraph = beforeContext {
    graph = TestGraphs.g2_mutual
    graphUtils = new GraphUtils(graph)
  }

  val threeNodeGraph = beforeContext {
    graph = TestGraphs.g3
    graphUtils = new GraphUtils(graph)
  }

  val sixNodeGraph = beforeContext {
    graph = TestGraphs.g6
    graphUtils = new GraphUtils(graph)
  }

  "two node graph with each following the other" definedAs twoNodeGraph should {
    "neighborCount should always be 1" in {
      graph.iterator foreach { node =>
        GraphDir.values foreach { dir =>
          graphUtils.neighborCount(node.id, dir) mustEqual 1
        }
      }
    }

    "random walk of one step with resetProb of 0" in {
      // with reset prob of 0 and 1 step forward, it should always
      val resetProb = 0.0
      val numWalkSteps = 2L
      val walkParams = GraphUtils.RandomWalkParams(
          numWalkSteps, resetProb, None, Some(10), None, false, GraphDir.OutDir, false)

      val (visitsCounter, pathsCounterOption) = graphUtils.randomWalk(OutDir, Seq(1), walkParams)
      val visitsCountMap = visitsCounter.infoAllNodes
      visitsCountMap.get(1) mustEqual 1
      visitsCountMap.get(2) mustEqual 1

      val pathsCountMap = pathsCounterOption.get.infoAllNodes
      pathsCountMap.get(1).toSeq mustEqual Array(DirectedPath(Array(1))).toSeq
      pathsCountMap.get(2).toSeq mustEqual Array(DirectedPath(Array(1, 2))).toSeq

      // random walk but no top paths maintained
      val (visitsCounter2, pathsCounterOption2) = graphUtils.randomWalk(OutDir, Seq(1),
          GraphUtils.RandomWalkParams(numWalkSteps, resetProb,
          None, None, None, false, GraphDir.OutDir, false))
      val visitCounterMap2 = visitsCounter2.infoAllNodes
      visitCounterMap2.get(1) mustEqual 1
      visitCounterMap2.get(2) mustEqual 1

      pathsCounterOption2.isDefined mustEqual false
    }

    "random walk of n steps with resetProb of 0" in {
      // with reset prob of 0 and 1 step forward, it should always
      val resetProb = 0.0
      var numTimesTested = 0
      for (numWalkSteps <- 2 to 8) {
        for (startNodeId <- 1 to 2) {
          numTimesTested += 1
          val othernd = if (startNodeId == 1) 2 else 1
          val walkParams = GraphUtils.RandomWalkParams(
              numWalkSteps, resetProb, None, Some(10), None, false, GraphDir.OutDir, false)

          val (visitsCounter, pathsCounterOption) = graphUtils.randomWalk(OutDir, Seq(startNodeId), walkParams)
          visitsCounter.infoAllNodes.get(startNodeId) mustEqual (numWalkSteps / 2 + (numWalkSteps % 2))
          visitsCounter.infoAllNodes.get(othernd) mustEqual numWalkSteps / 2
          pathsCounterOption.get.infoAllNodes.size mustEqual 2
        }
      }
      numTimesTested mustEqual (7 * 2)
    }
  }

  "three node graph" definedAs threeNodeGraph should {
    "bfs" in {
      val walkParams = GraphUtils.RandomWalkParams(
          5L, 0.0, None, Some(2), Some(5), false, GraphDir.OutDir, false)
      val visitsPerNode = graphUtils.calculateBFS(10, walkParams)._1
      visitsPerNode.size mustEqual 2
      visitsPerNode.get(11) mustEqual 3
      visitsPerNode.get(12) mustEqual 2
    }

  }

  "six node graph" definedAs sixNodeGraph should {

    "basic graph checks" in {
      graph.nodeCount mustEqual 6
      graph.edgeCount mustEqual 11
      graphUtils.neighborCount(10, GraphDir.OutDir) mustEqual 3
      graphUtils.neighborCount(11, GraphDir.InDir) mustEqual 2
    }

    "random walk of 1000 steps" in {
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
        val node = nodeIterator.nextInt
        (visitsCounter.infoAllNodes.get(node) >= minNumVisitsExpected) mustEqual true
      }
    }

    "personalized reputation" in {
      val walkParams = GraphUtils.RandomWalkParams(
          10000L, 0.5, None, Some(2), None, false, GraphDir.OutDir, false)
      val visitsPerNode = graphUtils.calculatePersonalizedReputation(10, walkParams)._1
      // 10000 steps should visit every node at least once
      visitsPerNode.size mustEqual graph.nodeCount
      val nodeIterator = visitsPerNode.keySet.iterator
      while (nodeIterator.hasNext) {
        val node = nodeIterator.nextInt
        (visitsPerNode.get(node) >= 1) mustEqual true
      }
      // TODO more testing here
    }

    "stable random walk correctly" in {
      val walkParams = GraphUtils.RandomWalkParams(
        10000L, 0.5, None, Some(2), None, false, GraphDir.OutDir, true)
      val visitsPerNode = graphUtils.calculatePersonalizedReputation(10, walkParams)._1
      val visitsPerNode2 = graphUtils.calculatePersonalizedReputation(10, walkParams)._1
      checkMapApproximatelyEquals(visitsPerNode, visitsPerNode2, 200) // Prob(fail) ~ 10^-7
    }

    "maxDepth works properly" in {
      val walkParams = GraphUtils.RandomWalkParams(7L, 0.0, None, Some(2), Some(1),
          false, GraphDir.OutDir, false)
      val visitsPerNode = graphUtils.calculatePersonalizedReputation(12, walkParams)._1
      visitsPerNode.size mustEqual 1
      (visitsPerNode.get(12) >= 1) mustEqual true
      val walkParams2 = GraphUtils.RandomWalkParams(8L, 0.0, None, Some(2), Some(2), false,
        GraphDir.OutDir, false)
      val visitsPerNode2 = graphUtils.calculatePersonalizedReputation(12, walkParams2)._1
      visitsPerNode2.size mustEqual 2
      (visitsPerNode2.get(12) >= 1) mustEqual true
      (visitsPerNode2.get(14) >= 1) mustEqual true
    }

    "bfs" in {
      val walkParams = GraphUtils.RandomWalkParams(9L, 0.0, None, Some(2), Some(5), false,
        GraphDir.OutDir, false)
      val visitsPerNode = graphUtils.calculateBFS(15, walkParams)._1
      // doing enough random steps to cause to visit every node (even with reset prob of 0.5)
      visitsPerNode.size mustEqual 5
      visitsPerNode.get(12) mustEqual 3
      visitsPerNode.get(11) mustEqual 2
      visitsPerNode.get(14) mustEqual 2
      visitsPerNode.get(10) mustEqual 1
      visitsPerNode.get(13) mustEqual 1
    }
  }

  "random walk on a large graph" should {
    graph = TestGraphs.generateRandomGraph(10 * 1000, 100)
    graphUtils = new GraphUtils(graph)

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
        val (walk, duration) = Duration.inMilliseconds {
          graphUtils.randomWalk(OutDir, Seq(startNodeId), walkParams)
        }

        val (visitsCounter, pathsCounterOption) = walk
        visitsCounter.infoAllNodes.size must be_> (graph.getNodeById(startNodeId).get.outboundCount)
        if (times > ignoreFirstNum) {
          sumDuration += duration.inMilliseconds
        }
      }
      println("Avg duration over %d random walks: %s ms".format(numTimes, sumDuration/numTimes))
      true
    }
  }

  private def checkMapApproximatelyEquals(visitsPerNode: Int2IntMap, visitsPerNode2: Int2IntMap, delta: Int) {
    visitsPerNode.size mustEqual visitsPerNode2.size

    val nodeIterator = visitsPerNode.keySet.iterator
    while (nodeIterator.hasNext) {
      val node = nodeIterator.nextInt
      (visitsPerNode.get(node) - visitsPerNode2.get(node) < delta) mustBe true
    }
  }
}
