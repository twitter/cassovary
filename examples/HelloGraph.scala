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
