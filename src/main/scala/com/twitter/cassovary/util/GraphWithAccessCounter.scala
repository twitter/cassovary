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

package com.twitter.cassovary.util

import com.twitter.cassovary.graph.DirectedGraph
import net.lag.logging.Logger
import java.io._

/**
 * Graph which keeps an access counter of how many times a given node is accessed.
 * @param g Input graph
 * @param statsInterval How often to write stats to disk
 * @param outputDirectory Directory to write stats to
 */
class GraphWithAccessCounter(val g: DirectedGraph, val statsInterval: Long,
                             val outputDirectory: String) extends DirectedGraph {

  var counter = new Array[Int](g.maxNodeId+1)
  var accesses: Long = 0
  var writes = 0

  protected val log = Logger.get("GraphWithAccessCounter")

  override def getNodeById(id: Int) = {
    counter(id) += 1
    accesses += 1
    if (accesses % statsInterval == 0) {
      log.info("Now writing access stats")
      writeStats("%s/%s.txt".format(outputDirectory, writes))
      writes += 1
      log.info("Done writing access stats %s".format(writes))
      resetStats
    }

    g.getNodeById(id)
  }

  override def existsNodeId(id: Int) = g.existsNodeId(id)
  def nodeCount = g.nodeCount
  val storedGraphDir = g.storedGraphDir
  def edgeCount = g.edgeCount
  def iterator = g.iterator

  def writeStats(fileName: String) {
    FileUtils.printToFile(new File(fileName))(p => {
      counter.foreach({ i => p.println(i) })
    })
  }

  def getStats = counter

  // Zeroes the counter
  private def resetStats = { counter = new Array[Int](g.maxNodeId+1) }

  def graph = g
}
