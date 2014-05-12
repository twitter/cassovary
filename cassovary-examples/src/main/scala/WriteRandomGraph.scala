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

import com.twitter.cassovary.graph.TestGraphs
import com.twitter.cassovary.util.io.GraphWriter
import java.io.{File,FileWriter}
import scala.math

object WriteRandomGraph {

  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 50
    val avgOutDegree = math.ceil(math.log(numNodes)).toInt

    printf("Generating Erdos-Renyi random graph with n=%d nodes and log(n)=%d avg outdegree...\n", numNodes, avgOutDegree)
    val genGraph = TestGraphs.generateRandomGraph(numNodes,
      TestGraphs.getProbEdgeRandomDirected(numNodes, avgOutDegree))

    // Write graph to temporary file.
    val renumGraphDirName = System.getProperty("java.io.tmpdir")
    val renumGraphFileName = "erdos_renyi_" + numNodes + ".txt"
    val renumGraphFile = new File(renumGraphDirName, renumGraphFileName)
    printf("Writing graph to file %s.\n", renumGraphFile)
    GraphWriter.writeDirectedGraph(genGraph, new FileWriter(renumGraphFile))
    printf("Finished writing graph to file %s.\n", renumGraphFile)
  }
}
