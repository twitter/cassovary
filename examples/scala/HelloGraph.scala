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

/**
 * A very simple example that generates a complete 3-node directed graph
 * and prints a few stats of this graph.
 */

import com.twitter.cassovary.graph.TestGraphs

object HelloGraph {
  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 3
    printf("Generating a complete directed graph with %d nodes...\n", numNodes)
    val graph = TestGraphs.generateCompleteGraph(numNodes)
    printf("\nHello Graph!\n\tA complete directed graph with %s nodes has %s directed edges.\n",
      graph.nodeCount, graph.edgeCount)
  }
}
