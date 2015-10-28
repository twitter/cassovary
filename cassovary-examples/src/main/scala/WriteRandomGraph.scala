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
 * and writes it to a file.
 */

import com.twitter.cassovary.graph.{StoredGraphDir, TestGraphs}
import com.twitter.cassovary.util.io.GraphWriter
import java.io.{File,FileWriter}

object WriteRandomGraph {

  // Arguments: <graphDirectory> <prefix of file names> <# of nodes> <average #edges/node> <number of file chunks>
  def main(args: Array[String]) {
    val (graphDirectory, prefix, numNodes, avgOutDegree, numFileChunks) = if (args.length > 0) {
      (args(0), args(1), args(2).toInt, args(3).toDouble, args(4).toInt)
    } else ("/tmp", "erdos_renyi", 50, 10.0, 1)
    //val avgOutDegree = numEdges.toDouble/numNodes //math.ceil(math.log(numNodes)).toInt

    printf("Generating Erdos-Renyi random graph with %d nodes and %s avg outdegree...\n",
      numNodes, avgOutDegree)
    val genGraph = TestGraphs.generateRandomGraph(numNodes,
      TestGraphs.getProbEdgeRandomDirected(numNodes, avgOutDegree), StoredGraphDir.OnlyOut)

    // Write graph to temporary file.
    val graphFiles = (0 until numFileChunks) map { i => prefix + "_" + i + ".txt" }
    val fileWriters = graphFiles map { name =>
      new FileWriter(new File(graphDirectory, name))
    }
    printf("Writing graph with %d nodes and %s edges in dir %s to file(s) %s\n",
      genGraph.nodeCount, genGraph.edgeCount, graphDirectory, graphFiles.mkString(","))
    GraphWriter.writeDirectedGraph(genGraph, fileWriters, false, false)
    printf("Finished writing graph to files.\n")
  }
}
