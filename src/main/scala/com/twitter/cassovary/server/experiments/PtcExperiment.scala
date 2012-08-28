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
package com.twitter.cassovary.server.experiments

import com.twitter.cassovary.server.{CachedDirectedGraphServerConfig, CachedDirectedGraphServerExperiment}
import com.twitter.cassovary.graph.{DirectedPath, GraphUtils, GraphDir, CachedDirectedGraph}
import com.twitter.cassovary.graph.GraphUtils.RandomWalkParams
import java.io.File
import com.twitter.cassovary.util.{FileUtils, ExecutorUtils}
import java.util.concurrent.{Future, Executors}
import net.lag.logging.Logger
import scala.collection.JavaConversions._

class PtcExperiment(config: CachedDirectedGraphServerConfig,
                    graph:CachedDirectedGraph)
    extends CachedDirectedGraphServerExperiment(config, graph) {

  private val log = Logger.get(getClass.getName)

  val nodeList = config.nodeList
  val verbose = config.verbose

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


  def run {
    // Do a random walk
    val walkParams = if (verbose)
      RandomWalkParams(10000, 0.0, Some(2000), Some(1), Some(3), false, GraphDir.OutDir, false, true)
    else
      RandomWalkParams(10000, 0.0, Some(2000), None, Some(3), false, GraphDir.OutDir, false, true)

    val nodeFile = new File(nodeList)
    if (nodeFile.isDirectory) {
      val filelist = nodeFile.list
      log.info("Concurrent Start with %s threads!".format(filelist.size))
      val futures = ExecutorUtils.parallelWork[String, Unit](Executors.newFixedThreadPool(filelist.size), filelist,
      { file =>
        val cGraph = graph.getThreadSafeChild
        val graphUtils = new GraphUtils(cGraph)

        if (verbose)
          ptcVerbose(nodeList+"/"+file, cGraph, graphUtils, walkParams)
        else
          ptc(nodeList+"/"+file, cGraph, graphUtils, walkParams)
      })
      futures.toArray.map { f => f.asInstanceOf[Future[Unit]].get }
    }
    else {
      log.info("Single Threaded Start!")
      val graphUtils = new GraphUtils(graph)
      if (verbose)
        ptcVerbose(nodeList, graph, graphUtils, walkParams)
      else
        ptc(nodeList, graph, graphUtils, walkParams)
    }

    log.info("Finished starting up!")
  }

}
