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

import com.twitter.cassovary.graph._
import com.twitter.cassovary.graph.GraphDir._
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.util.GraphWithSimulatedCache
import com.twitter.util.Duration
import scala.util.Random
import scala.collection.mutable

object RandomWalkCache {
  def main(args: Array[String]) {
    val numNodes = if (args.length > 0) args(0).toInt else 3

    // Run an algorithm innerFn on a graph for different cache sizes and mechanisms,
    // printing the average miss rate for each cache mechanism and size
    def iterate (innerFn: (Graph => Unit)) = {
      // Table of results
      val table = new mutable.HashMap[(String,Double),Array[Double]]()
      val output = new mutable.StringBuilder()
      // Iterate 10 times, foreach cache mechanism, foreach cache size (10%-100%)
      val rand = new Random()
      for (i <- 1 to 1) {
        val randomSeed = rand.nextLong()
        Seq("lru", "mru", "clock") map { cacheMechanism:String =>
          output.append(cacheMechanism)
          (0.1 until 1.1 by 0.1) map { cachePct:Double =>
            if (!table.contains((cacheMechanism, cachePct))) {
              table.put((cacheMechanism, cachePct), new Array[Double](10))
            }

            // Generate graph with constant random seed for each "cycle"
            val graphInner = TestGraphs.generateRandomGraphWithSeed(numNodes, 10 min numNodes, randomSeed)
            val graph = new GraphWithSimulatedCache(graphInner, (cachePct*numNodes).toInt, cacheMechanism)

            innerFn(graph)

            table((cacheMechanism,cachePct)).update(i-1, graph.getStats(false)._3)
            output.append(" %s".format(graph.getStats(false)))
          }
          output.append("\n")
        }
      }
      // Print out regular
      printf("%s", output.toString())
      // Print out averages
      printf("Means\n")
      table.toList.sortBy{_._1} map { x =>
        printf("%s %s %s\n", x._1._1, x._1._2, x._2.foldLeft(0.0)(_ + _) / x._2.length)
      }
    }

    def bfsShortestPaths(startNodeId: Int, graph:DirectedGraph, dir:GraphDir) = {
      val stack = new Array[(Int,Int)](graph.nodeCount) // Pseudo-stack, also the array to return
      val seen = new mutable.HashMap[Int,Int]() // Which nodes have been seen?
      var stackPtr = 0 // Next slot in the stack to look at
      var stackEnd = 1 // Next free slot in the stack
      stack.update(stackPtr, (startNodeId, 0))
      seen.put(startNodeId, 1)
      while (stackPtr < graph.nodeCount && stack(stackPtr) != null) {
        val (currNodeId, currDepth) = stack(stackPtr)
        val currNode = graph.getNodeById(currNodeId).get
        currNode.neighborIds(dir) foreach { neighborId =>
          if (!seen.contains(neighborId)) {
            stack.update(stackEnd, (neighborId, currDepth+1))
            stackEnd += 1
            seen.put(neighborId, 1)
          }
        }
        stackPtr += 1
      }
      stack
    }

    iterate { graph:Graph =>
      val numSteps = 1000L * 1000L
      val walkParams = RandomWalkParams(numSteps, 0.1, None, Some(2), None, false, GraphDir.OutDir, true)
      val graphUtils = new GraphUtils(graph)
      for (j <- 0 to 0) {
        val ((topNeighbors, paths), duration) = Duration.inMilliseconds {
          graphUtils.calculatePersonalizedReputation(j, walkParams)
        }
      }
    }

//    iterate { graph:DirectedGraph =>
//      for (j <- 0 to graph.nodeCount-1) {
//        bfsShortestPaths(j, graph, GraphDir.OutDir)
//      }
//    }

  }
}
