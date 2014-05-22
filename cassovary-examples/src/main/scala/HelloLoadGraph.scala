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
 * A very simple example that loads in a graph from a file and generates stats
 *
 * Specifically, it loads an ArrayBasedDirectedGraph in using AdjacencyListGraphReader,
 * and configures the reader to use 2 threads to load the graph instead of just one.
 * This example loads in both toy_6nodes_adj_1.txt and toy_6nodes_adj_2.txt from
 * src/test/resources/graphs/
 */

import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import com.twitter.cassovary.util.NodeNumberer
import java.util.concurrent.Executors

object HelloLoadGraph {
  def main(args: Array[String]) {
    val threadPool = Executors.newFixedThreadPool(2)
    val graph = AdjacencyListGraphReader.forIntIds("cassovary-core/src/test/resources/graphs/", "toy_6nodes_adj",
      threadPool).toArrayBasedDirectedGraph()

    printf("\nHello Graph!\n\tA graph loaded from two adjacency list files with %s nodes has %s directed edges.\n",
      graph.nodeCount, graph.edgeCount)
    threadPool.shutdown()
  }
}
