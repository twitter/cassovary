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

class GraphWithAccessCounter(val g: DirectedGraph, val statsInterval: Long,
                             val outputDirectory: String) extends DirectedGraph {

  var counter = new Array[Int](g.maxNodeId+1)
  var accesses: Long = 0
  var writes = 0

  protected val log = Logger.get

  override def getNodeById(id: Int) = {
    counter(id) += 1
    accesses += 1
    if (accesses % statsInterval == 0) {
      log.info("simcache now writing access stats")
      writeStats("%s/%s.txt".format(outputDirectory, writes))
      writes += 1
      log.info("simcache done writing access stats %s".format(writes))
      resetStats
    }

    g.getNodeById(id)
  }

  override def existsNodeId(id: Int) = g.existsNodeId(id)
  def nodeCount = g.nodeCount
  val storedGraphDir = g.storedGraphDir
  def edgeCount = g.edgeCount
  def iterator = g.iterator

  def writeStats(fileName: String) = {
    printToFile(new File(fileName))(p => {
      counter.foreach({ i => p.println(i) })
    })
  }

  def getStats = { counter }

  private def resetStats = { counter = new Array[Int](g.maxNodeId+1) }

  def graph = g

  private def printToFile(f: java.io.File)(op: java.io.PrintWriter => Unit) {
    val p = new java.io.PrintWriter(f)
    try { op(p) } finally { p.close() }
  }

}
