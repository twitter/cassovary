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

/**
 * Generates a directed Erdos-Renyi random graph file with n log(n) edges 
 * and node ids distributed uniformly throughout the space of 0..MaxNodeId. 
 * Loads the graph file both with a SequentialNodeNumberer and without,
 * and compares approximate representation sizes of the two.
 */

import com.google.common.util.concurrent.MoreExecutors
import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import com.twitter.cassovary.util.SequentialNodeNumberer
import com.twitter.cassovary.graph.TestGraphs
import java.io.{File,PrintWriter}
import scala.math
import scala.util.Random

object RenumberedGraph {

  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 50
    val avgOutDegree = math.ceil(math.log(numNodes)).toInt
    val MaxNodeId = 1 << 20

    printf("Generating Erdos-Renyi random graph with n=%d nodes and log(n)=%d avg outdegree...\n", numNodes, avgOutDegree)
    // Generate mapping of each node id to a random integer in the space 0..MaxNodeId.
    val rng = new Random()
    val nodeIds = Array.fill(numNodes) { math.abs(rng.nextInt(MaxNodeId)) }

    val genGraph = TestGraphs.generateRandomGraph(numNodes,
      TestGraphs.getProbEdgeRandomDirected(numNodes, avgOutDegree))

    // Write graph to temporary file, mapping all node ids from dense to sparse representation.
    val renumGraphDirName = System.getProperty("java.io.tmpdir")
    val renumGraphFileName = "erdos_renyi_" + numNodes + ".txt"
    val renumGraphFile = new File(renumGraphDirName, renumGraphFileName)
    printf("Writing graph to temporary file %s with node ids distributed across 0..%d\n", renumGraphFile, MaxNodeId)
    val gWriter = new PrintWriter(renumGraphFile)
    genGraph.foreach { v => {
        gWriter.println(nodeIds(v.id) + " " + v.outboundCount)
        v.outboundNodes().foreach { ngh =>
          gWriter.println(nodeIds(ngh))
        }
      }
    }
    gWriter.close()

    // Read graph file into memory with renumbering.
    val readGraph = new AdjacencyListGraphReader(renumGraphDirName, renumGraphFileName, new SequentialNodeNumberer[Int]()) {
      override val executorService = MoreExecutors.sameThreadExecutor()
    }.toArrayBasedDirectedGraph()

    val rgComplexity = readGraph.approxStorageComplexity
    printf("A renumbered graph with %d nodes (min id: %d, max id: %d) and %d directed edges has an approx. storage complexity of %d bytes.\n",
      readGraph.nodeCount, readGraph.map{_.id}.min, readGraph.map{_.id}.max, readGraph.edgeCount, rgComplexity)
    printf("First 3 nodes of renumbered graph: %s\n", readGraph.toString(3))

    // Read graph file into memory without renumbering.
    val readGraph2 = new AdjacencyListGraphReader(renumGraphDirName, renumGraphFileName) {
      override val executorService = MoreExecutors.sameThreadExecutor()
    }.toArrayBasedDirectedGraph()
    val rg2Complexity = readGraph2.approxStorageComplexity
    printf("An unrenumbered graph with %d nodes (min id: %d, max id: %d) and %d directed edges has an approx. storage complexity of %d bytes.\n",
      readGraph2.nodeCount, readGraph2.map{_.id}.min, readGraph2.map{_.id}.max, readGraph2.edgeCount, rg2Complexity)

    renumGraphFile.delete()

    val complexityDiff = rg2Complexity - rgComplexity
    printf("Savings for %d node graph with MaxNodeId %d: %d bytes (%.2f%%)\n", numNodes, MaxNodeId, complexityDiff, 100.0 * complexityDiff.toFloat / rgComplexity)

    printf("Finished running RenumberedGraph example.\n")

  }
}
