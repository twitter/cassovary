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

/**
 * A simple example that generates a random graph and runs a random walk
 * personalized to a node in the graph.
 */

import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, TestGraphs}
import com.twitter.util.{Duration, Stopwatch}
import scala.collection.JavaConversions._

object RandomWalk {
  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 3
    printf("Generating a random graph with %d nodes...\n", numNodes)
    val graph = TestGraphs.generateRandomGraph(numNodes,
      TestGraphs.getProbEdgeRandomDirected(numNodes, 10 min numNodes))
    printf("\nGenerated a random directed graph with %s nodes and %s edges.\n",
      graph.nodeCount, graph.edgeCount)

    val numSteps = 1000L * 1000L
    val walkParams = RandomWalkParams(numSteps, 0.1, None, Some(2))
    val graphUtils = new GraphUtils(graph)
    printf("Now doing a random walk of %s steps from Node 0...\n", numSteps)
    val elapsed = Stopwatch.start()
    val (topNeighbors, paths) = graphUtils.calculatePersonalizedReputation(0, walkParams)
    val duration = elapsed()
    printf("Random walk visited %s nodes in %s ms:\n", topNeighbors.size, duration.inMillis.toInt)
    printf("%8s%10s\t%s\n", "NodeId", "#Visits", "Top 2 Paths with counts")
    topNeighbors.toList.sortWith((x1, x2) => x2._2.intValue < x1._2.intValue).take(10).foreach { case (id, numVisits) =>
      if (paths.isDefined) {
        val topPaths = paths.get(id).map { case (DirectedPath(nodes), count) =>
          nodes.mkString("->") + " (%s)".format(count)
        }
        printf("%8s%10s\t%s\n", id, numVisits, topPaths.mkString(" | "))
      }
    }
  }
}
