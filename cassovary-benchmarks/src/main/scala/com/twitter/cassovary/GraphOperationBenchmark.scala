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
package com.twitter.cassovary

import com.twitter.cassovary.algorithms.{PageRankParams, PageRank}
import com.twitter.cassovary.graph.{DirectedGraph,GraphDir, GraphUtils, NodeUtils}
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.util.Duration
import com.twitter.util.Stopwatch
import scala.collection.JavaConversions._
import scala.util.Random

trait GraphOperationBenchmark {
  def graph : DirectedGraph
  def operation(g : DirectedGraph)

  def name : String

  def run(repetitions : Int = 1) : Duration = {
    (1 to repetitions).map {
      _ =>
        val watch = Stopwatch.start()
        operation(graph)
        watch()
    }.reduce(_ + _) / repetitions
  }
}

class PageRankBenchmark(val graph: DirectedGraph, pageRankParams : PageRankParams = PageRankParams())
    extends GraphOperationBenchmark {
  override def name = "PageRank"

  override def operation(graph : DirectedGraph) {
    PageRank(graph, pageRankParams)
  }
}

class PersonalizedPageRankBenchmark(val graph : DirectedGraph,
                                    randomWalkParams : RandomWalkParams = RandomWalkParams(20, 0.3))
    extends GraphOperationBenchmark {
  val rng = new Random

  val graphUtils = new GraphUtils(graph)

  override def name = "Personalized PageRank"

  def randomNodeIds() : Seq[Int] = {
    graphUtils.randomWalk(GraphDir.OutDir,
      Seq(graph.maxNodeId), RandomWalkParams(10, 0.0))._1.infoAllNodes.keySet()
        .iterator().map(i => i :Int).toSeq
  }

  override def operation(g: DirectedGraph) {
    graphUtils.calculatePersonalizedReputation(NodeUtils.pickRandNodeId(randomNodeIds(), rng),
      randomWalkParams)
  }
}
