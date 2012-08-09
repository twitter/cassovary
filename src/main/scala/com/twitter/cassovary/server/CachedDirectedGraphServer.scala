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
package com.twitter.cassovary.server

import com.twitter.ostrich.admin.Service
import com.twitter.cassovary.util.GraphLoader
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, GraphDir}
import util.Random
import scala.collection.JavaConversions._
import com.twitter.logging.Logger

/**
 * "Server" that loads the graph and runs some arbitrary code, like PersonalizedReputation
 * @param config
 */
class CachedDirectedGraphServer(config: CachedDirectedGraphServerConfig) extends Service {
  private val log = Logger.get(getClass.getName)

  def start() {
    log.info("Starting up...")

    // Load the desired graph
    val graph = GraphLoader("/Volumes/Macintosh HD 2/graph_dump/current/OnlyOut",
      "lru", 1000000, 200000000, "/tmp/shards", 256, 16, true, "/tmp/cached")

//    val graph = GraphLoader("/Volumes/Macintosh HD 2/graph_dump_random",
//      "lru", 1000000, 200000000, "/tmp/shards_random", 256, 16, true, "/tmp/cached_random")

    // Do a random walk
    val walkParams = RandomWalkParams(10000, 0.0, Some(1000), None, Some(3))
    val graphUtils = new GraphUtils(graph)

    val rand = new Random()
    var j = 0
    rand.shuffle((1 to 3000000).toSeq).foreach { i =>
      j += 1
      log.info("PR for %d (id %d)".format(j, i))
      graphUtils.calculatePersonalizedReputation(i, walkParams)
      if (j % 1000 == 0) {
        log.info("cache_miss_stats: " + graph.statsString)
      }
//      val (topNeighbors, paths) = graphUtils.calculatePersonalizedReputation(i, walkParams)
//      topNeighbors.toList.sort((x1, x2) => x2._2.intValue < x1._2.intValue).take(10).foreach { case (id, numVisits) =>
//        if (paths.isDefined) {
//          val topPaths = paths.get.get(id).map { case (DirectedPath(nodes), count) =>
//            nodes.mkString("->") + " (%s)".format(count)
//          }
//          log.info("%8s%10s\t%s\n", id, numVisits, topPaths.mkString(" | "))
//        }
//      }
    }

    //    val randomGraph = TestGraphs.generateRandomGraph(1000000, 200)
    //    randomGraph.writeToDirectory("/Volumes/Macintosh HD 2/graph_dump_random", 8)

    log.info("Finished starting up!")
  }

  def shutdown() {
    log.info("Shutting Down~!")
  }
}
