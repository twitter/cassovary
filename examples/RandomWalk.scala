/**
 * A simple example that generates a random graph and runs a random walk
 * personalized to a node in the graph.
 */

import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, TestGraphs}
import com.twitter.util.Duration

object RandomWalk {
  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 3
    printf("Generating a random graph with %d nodes...\n", numNodes)
    val graph = TestGraphs.generateRandomGraph(numNodes, 10 min numNodes)
    printf("\nGenerated a random directed graph with %s nodes and %s edges.\n",
      graph.nodeCount, graph.edgeCount)

    val numSteps = 1000L * 1000L
    val walkParams = RandomWalkParams(numSteps, 0.1, None, Some(2))
    val graphUtils = new GraphUtils(graph)
    printf("Now doing a random walk of %s steps from Node 0...\n", numSteps)
    val ((topNeighbors, paths), duration) = Duration.inMilliseconds {
      graphUtils.calculatePersonalizedReputation(0, walkParams)
    }
    printf("Random walk visited %s nodes in %s ms:\n", topNeighbors.size, duration.inMillis.toInt)
    printf("%8s%10s\t%s\n", "NodeId", "#Visits", "Top 2 Paths with counts")
    topNeighbors.take(10).foreach { case (id, numVisits) =>
      val topPaths = paths.get(id).get.map { case (DirectedPath(nodes), count) =>
        nodes.mkString("->") + " (%s)".format(count)
      }
      printf("%8s%10s\t%s\n", id, numVisits, topPaths.mkString(" | "))
    }
  }
}
