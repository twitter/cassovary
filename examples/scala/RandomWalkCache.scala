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
 * A simple example that generates a random graph and runs a random walk
 * personalized to a node in the graph.
 */

import com.twitter.cassovary.graph.GraphDir
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, TestGraphs}
import com.twitter.util.Duration
import scala.collection.JavaConversions._
import scala.collection.mutable
import scala.util.Random

object RandomWalkCache {
  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 3
    // printf("Generating a random graph with %d nodes...\n", numNodes)

    val table = new mutable.HashMap[(String,Double),Array[Double]]()

    val rand = new Random()
    for (i <- 1 to 2) {
      val randomSeed = rand.nextLong()
      Seq("lru", "mru", "clock") map { cacheMechanism:String =>
        printf("%s ", cacheMechanism)
        (0.1 until 1.1 by 0.1) map { cachePct:Double =>
          if (!table.contains((cacheMechanism, cachePct))) {
            table.put((cacheMechanism, cachePct), new Array[Double](10))
          }

          val graph = TestGraphs.generateRandomGraphWithSimulatedCache(numNodes, 10 min numNodes,
            (cachePct*numNodes).toInt, cacheMechanism, randomSeed)
          // printf("\nGenerated a random directed graph with %s nodes and %s edges.\n",
          //  graph.nodeCount, graph.edgeCount)

          val numSteps = 1000L * 1000L
          val walkParams = RandomWalkParams(numSteps, 0.1, None, Some(2), None, false, GraphDir.OutDir, true)
          val graphUtils = new GraphUtils(graph)
          //    printf("Now doing a random walk of %s steps from Node 0...\n", numSteps)
          val ((topNeighbors, paths), duration) = Duration.inMilliseconds {
            graphUtils.calculatePersonalizedReputation(0, walkParams)
          }
          //    printf("Random walk visited %s nodes in %s ms:\n", topNeighbors.size, duration.inMillis.toInt)
          //    printf("%8s%10s\t%s\n", "NodeId", "#Visits", "Top 2 Paths with counts")
          //    topNeighbors.toList.sort((x1, x2) => x2._2.intValue < x1._2.intValue).take(10).foreach { case (id, numVisits) =>
          //      if (paths.isDefined) {
          //        val topPaths = paths.get.get(id).map { case (DirectedPath(nodes), count) =>
          //          nodes.mkString("->") + " (%s)".format(count)
          //        }
          //        printf("%8s%10s\t%s\n", id, numVisits, topPaths.mkString(" | "))
          //      }
          //    }

          table((cacheMechanism,cachePct)).update(i-1, graph.getStats(false))
          printf(" ")
        }
        printf("\n")
      }
    }

    // Print out averages
    table.toList.sortBy{_._1} map { x =>
      printf("%s %s %s\n", x._1._1, x._1._2, x._2.foldLeft(0.0)(_ + _) / x._2.length)
    }

  }
}
