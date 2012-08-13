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

/**
 * "Server" that loads the graph and runs some arbitrary code, like PersonalizedReputation
 * @param config
 */
class CachedDirectedGraphServer(config: CachedDirectedGraphServerConfig) extends Service {
  private val log = Logger.get(getClass.getName)

  def start() {
    log.info("Starting up...")

    val nodeList = "/Volumes/Macintosh HD 2/mappedrand.txt"
    val verbose = false

    // Load the desired graph
    val graph = GraphLoader("/Volumes/Macintosh HD 2/graph_dump/current/OnlyOut",
      "lru", 1000000, 200000000, Array("/tmp/shards"), 256, 16, true, "/tmp/cached")

//    val graph = GraphLoader("/Volumes/Macintosh HD 2/graph_dump_random",
//      "lru", 1000000, 200000000, "/tmp/shards_random", 256, 16, true, "/tmp/cached_random")

    // Do a random walk
    val walkParams = if (verbose)
      RandomWalkParams(10000, 0.0, Some(2000), Some(1), Some(3), false, GraphDir.OutDir, false, true)
    else
      RandomWalkParams(10000, 0.0, Some(2000), None, Some(3), false, GraphDir.OutDir, false, true)
    val graphUtils = new GraphUtils(graph)

    val nodeFile = new File(nodeList)
    if (nodeFile.isDirectory) {
      val filelist = nodeFile.list
      log.info("Concurrent Start with %s threads!".format(filelist.size))
      val futures = ExecutorUtils.parallelWork[String, Unit](Executors.newFixedThreadPool(filelist.size), filelist,
      { file =>
        if (verbose)
          ptcVerbose(nodeList+"/"+file, graph, graphUtils, walkParams)
        else
          ptc(nodeList+"/"+file, graph, graphUtils, walkParams)
      })
      futures.toArray.map { f => f.asInstanceOf[Future[Unit]].get }
    }
    else {
      log.info("Single Threaded Start!")
      if (verbose)
        ptcVerbose(nodeList, graph, graphUtils, walkParams)
      else
        ptc(nodeList, graph, graphUtils, walkParams)
    }

    log.info("Finished starting up!")
  }

  def ptc(nodeList: String, graph: CachedDirectedGraph, graphUtils: GraphUtils, walkParams: RandomWalkParams) {
    var j = 0
    FileUtils.linesFromFile(nodeList) { line =>
      val i = line.toInt
      if (graph.existsNodeId(i)) {
        j += 1
        log.info("PR for %d (id %d)".format(j, i))
        graphUtils.calculatePersonalizedReputation(i, walkParams)
        if (j % 1000 == 0) {
          log.info("cache_miss_stats: " + graph.statsString)
        }
      }
    }
  }

  def ptcVerbose(nodeList: String, graph: CachedDirectedGraph, graphUtils: GraphUtils, walkParams: RandomWalkParams) {
    var j = 0
    FileUtils.linesFromFile(nodeList) { line =>
      val i = line.toInt
      if (graph.existsNodeId(i)) {
        j += 1
        val sb = new StringBuilder
        sb.append("PR for %d (id %d)\n".format(j, i))
        val (topNeighbors, paths) = graphUtils.calculatePersonalizedReputation(i, walkParams)
        topNeighbors.toList.sort((x1, x2) => x2._2.intValue < x1._2.intValue).take(10).foreach { case (id, numVisits) =>
          if (paths.isDefined) {
            val topPaths = paths.get.get(id).map { case (DirectedPath(nodes), count) =>
              nodes.mkString("->") + " (%s)".format(count)
            }
            sb.append("%8s%10s\t%s\n".format(id, numVisits, topPaths.mkString(" | ")))
          }
        }
        log.info(sb.toString)
        if (j % 1000 == 0) {
          log.info("cache_miss_stats: " + graph.statsString)
        }
      }
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
