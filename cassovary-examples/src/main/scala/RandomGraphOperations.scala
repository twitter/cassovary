/*
 * Copyright 2013 Twitter, Inc.
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

import com.twitter.cassovary.graph.{DirectedGraphUtils, TestGraphs}
import scala.math

/**
 * Some operations on random graphs
 */

object RandomGraphOperations {

  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 50
    val avgOutDegree = if (args.length > 1) args(1).toInt else math.ceil(math.log(numNodes)).toInt

    printf("Generating Erdos-Renyi random directed graph with n=%d nodes and avg outdegree=%d...\n",
      numNodes, avgOutDegree)
    val graph = TestGraphs.generateRandomGraph(numNodes, avgOutDegree.toDouble / (numNodes - 1))
    //println(graph)

    println("Now computing the number of mutual edges of this graph...")
    val graphUtils = new DirectedGraphUtils(graph)
    val numMutualEdges = graphUtils.getNumMutualEdges
    printf("Number of mutual edges found = %d (%.2f%% of all %d edges)\n", numMutualEdges,
      (100.0 * numMutualEdges/graph.edgeCount), graph.edgeCount)
  }
}
