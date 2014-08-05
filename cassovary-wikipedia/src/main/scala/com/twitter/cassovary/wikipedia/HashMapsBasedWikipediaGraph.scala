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

package com.twitter.cassovary.wikipedia

import com.twitter.cassovary.graph.DirectedGraph
import com.twitter.cassovary.util.{SequentialNodeNumberer, NodeNumberer}
import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import com.twitter.app.Flags
import com.twitter.ostrich.stats.Stats
import com.twitter.logging.Logger
import scala.concurrent.duration.Duration
import java.util.concurrent.TimeUnit
import com.twitter.util.Stopwatch

class HashMapsBasedWikipediaGraph(val graph: DirectedGraph, val nodesNumberer: NodeNumberer[String],
                                  val disambiguation: HashArraysMultipleNames) extends WikipediaGraph {

}

object HashMapsBasedWikipediaGraph extends App {
  private val log = Logger.get("Reading Wikipedia to HashMaps")

  def readFromFiles(graphDirectory: String, graphPrefix: String, disambiguitionsFile: String): WikipediaGraph = {
    val nodeNumberer = new SequentialNodeNumberer[String]()
    log.info("Reading graph")
    val graph = new AdjacencyListGraphReader(graphDirectory, graphPrefix, nodeNumberer, identity)
      .toArrayBasedDirectedGraph()
    log.info("Reading disambiguition")
    val disambiguition = HashArraysMultipleNames.readFromFile(disambiguitionsFile)
    new HashMapsBasedWikipediaGraph(graph, nodeNumberer, disambiguition)
  }

  val flags = new Flags("Loading HashMaps based Wikipedia Graph")
  val directoryFlag = flags[String]("d", "Graph directory")
  val prefixFlag = flags[String]("p", "Graphs file prefix")
  val disambiguitionFlag = flags[String]("r", "Disambiguitions file")
  val helpFlag = flags("h", false, "Print usage")
  flags.parse(args)


  val watch = Stopwatch.start()
  readFromFiles(directoryFlag(), prefixFlag(), disambiguitionFlag())

  log.info("Wikipedia read successfully in " + watch())
}
