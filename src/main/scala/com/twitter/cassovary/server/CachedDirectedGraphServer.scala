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
import com.twitter.cassovary.util.{FileUtils, ExecutorUtils, GraphLoader}
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph._
import experiments.PtcExperiment
import scala.util.Random
import scala.collection.JavaConversions._
import scala.io.Source
import java.io.File
import com.twitter.cassovary.graph.GraphDir
import com.twitter.cassovary.graph.CachedDirectedGraph
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import com.twitter.cassovary.graph.GraphUtils
import scala.Some
import java.util.concurrent.{Future, Executors}
import net.lag.logging.Logger

abstract class CachedDirectedGraphServerExperiment(val config: CachedDirectedGraphServerConfig,
                                                   graph:CachedDirectedGraph) {
  def run: Unit
}

/**
 * "Server" that loads the graph and runs some arbitrary code, like PersonalizedReputation
 * @param config
 */
class CachedDirectedGraphServer(config: CachedDirectedGraphServerConfig) extends Service {
  private val log = Logger.get(getClass.getName)

  def start() {
    log.info("Starting up...")

    log.info("Nodelist is %s Verbose is %s".format(config.nodeList, config.verbose))
    log.info("GraphDump is %s CacheType is %s".format(config.graphDump, config.cacheType))
    log.info("NumNodes is %s NumEdges is %s NumShards is %s NumRounds is %s".format(config.numNodes, config.numEdges, config.numShards, config.numRounds))
    log.info("ShardDirs is %s CacheDir is %s".format(config.shardDirectories.mkString(" "), config.cacheDirectory))
    log.info("Experiment is %s".format(config.experiment))

    // Load the desired graph
    val graph = GraphLoader(config.graphDump, config.cacheType,
      config.numNodes, config.numEdges, config.shardDirectories,
      config.numShards, config.numRounds, true, config.cacheDirectory)

//    val graph = GraphLoader("/Volumes/Macintosh HD 2/graph_dump_random",
//      "lru", 1000000, 200000000, "/tmp/shards_random", 256, 16, true, "/tmp/cached_random")

    config.experiment match {
      case "ptc" => new PtcExperiment(config, graph)
      case "generate_random_graph" => generateRandomGraph
      case _ => throw new Exception("Invalid experiment name provided")
    }
  }


  def generateRandomGraph = {
    val randomGraph = TestGraphs.generateRandomGraph(1000000, 200)
    randomGraph.writeToDirectory("/Volumes/Macintosh HD 2/graph_dump_random", 8)
  }

  def shutdown() {
    log.info("Shutting Down~!")
  }
}
