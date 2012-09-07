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
import java.io._
import net.lag.logging.Logger

/**
 * Graph which keeps an access counter of how many times a given node is accessed.
 * Access count correctness is only ensured if run in a single thread.
 *
 * @param graph Input graph
 * @param statsDumpInterval How often to write stats to disk
 * @param statsOutputDirectory Directory to write stats to
 */
class GraphWithAccessCounter(val graph: DirectedGraph, val statsDumpInterval: Long,
                             val statsOutputDirectory: String) extends DirectedGraph {

  var counter = new Array[Int](graph.maxNodeId+1)
  var accesses: Long = 0
  var writes = 0

  protected val log = Logger.get("GraphWithAccessCounter")

  override def getNodeById(id: Int) = {
    counter(id) += 1
    accesses += 1
    // TODO can make this concurrent
    if (accesses % statsDumpInterval == 0) {
      log.info("Writing access stats...")
      writeStats("%s/%s.txt".format(statsOutputDirectory, writes))
      writes += 1
      resetStats
    }

    graph.getNodeById(id)
  }

  override def existsNodeId(id: Int) = graph.existsNodeId(id)

  def nodeCount = graph.nodeCount

  val storedGraphDir = graph.storedGraphDir

  def edgeCount = graph.edgeCount

  def iterator = graph.iterator

  def writeStats(fileName: String) {
    FileUtils.printToFile(new File(fileName))(p => {
      counter.foreach({ i => p.println(i) })
    })
  }

  def getStats = counter

  private def resetStats = { counter = new Array[Int](graph.maxNodeId+1) }
}
