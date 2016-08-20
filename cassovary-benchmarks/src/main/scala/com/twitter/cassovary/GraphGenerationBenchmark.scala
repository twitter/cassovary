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

import com.twitter.app.Flags
import com.twitter.cassovary.graph.TestGraphs

/**
 * Performance test.
 *
 * Performs random graph generation algorithms and provides their running times.
 *
 * Options:
 *   -n [numNodes]  number of nodes in generated graphs (default: 10000)
 *   -p [probEdge]  probability of existence of edge in a graph (default: 0.001)
 */
object GraphGenerationBenchmark {
  val DEFAULT_REPS = 10

  val flags = new Flags("Graph generation benchmarks")
  val numNodesFlag = flags("n", 10000, "Number of nodes in generated graph")
  val probEdgeFlag = flags("p", 0.001, "Probability of edge existance")
  val repsFlag = flags("reps", DEFAULT_REPS, "Number of times to run benchmark")
  val helpFlag = flags("h", false, "Print usage")

  def performBenchmarks(numNodes: Int, probEdge: Double, reps: Int) {
    val benchmarks = List[GraphGenerationBenchmark](
      new GraphGenerationBenchmark(numNodes, probEdge) {
        override def name = "Random directed graph generation"

        override def operation() = TestGraphs.generateRandomGraph(numNodes, probEdge)
      },
      new GraphGenerationBenchmark(numNodes, probEdge) {
        override def name = "Random undirected graph generation"

        override def operation() = TestGraphs.generateRandomUndirectedGraph(numNodes, probEdge)
      }
    )
    for (benchmark <- benchmarks) {
      printf("Running benchmark %s with %d nodes and probEdge=%f\n", benchmark.name,
        benchmark.numNodes, benchmark.probEdge)
      val duration = benchmark.run(reps)
      printf("\tAvg time over %d repetitions: %s.\n", reps, duration)
    }
  }

  abstract case class GraphGenerationBenchmark(numNodes: Int, probEdge: Double)
    extends OperationBenchmark

  def main(args: Array[String]) {
    flags.parseArgs(args)

    if (helpFlag()) {
      println(flags.usage)
    } else {
      performBenchmarks(numNodesFlag(), probEdgeFlag(), repsFlag())
    }
  }
}
