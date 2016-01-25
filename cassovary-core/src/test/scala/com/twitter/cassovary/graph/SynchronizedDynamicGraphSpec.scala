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
package com.twitter.cassovary.graph

import java.util.concurrent.Executors
import org.scalatest.{Matchers, WordSpec}
import StoredGraphDir._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

class SynchronizedDynamicGraphSpec extends WordSpec with Matchers {
  implicit val ecctxt = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  "Add new nodes" in {
    val graph = new SynchronizedDynamicGraph()
    graph.nodeCount shouldEqual 0
    graph.edgeCount shouldEqual 0
    val node1 = graph.getOrCreateNode(1)
    node1.id shouldEqual 1
    graph.nodeCount shouldEqual 1
    graph.edgeCount shouldEqual 0

    val node2 = graph.getOrCreateNode(2)
    node2.id shouldEqual 2
    graph.nodeCount shouldEqual 2
    graph.edgeCount shouldEqual 0
  }

  def createTempGraph(storedGraphDir: StoredGraphDir) = {
    val graph = new SynchronizedDynamicGraph(storedGraphDir)
    graph.getOrCreateNode(1)
    graph.getOrCreateNode(2)
    graph
  }

  "Add new edges in OnlyIn graph" in {
    val graph = createTempGraph(StoredGraphDir.OnlyIn)
    graph.addEdge(0, 1)
    val node1 = graph.getNodeById(0).get
    node1.inboundNodes.toSeq shouldEqual Seq()
    node1.outboundNodes.toSeq shouldEqual Seq()
    val node2 = graph.getNodeById(1).get
    node2.inboundNodes.toSeq shouldEqual Seq(0)
    node2.outboundNodes.toSeq shouldEqual Seq()
  }

  "Add new edges in OnlyOut graph" in {
    val graph = createTempGraph(StoredGraphDir.OnlyOut)
    graph.addEdge(1, 2)
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toSeq shouldEqual Seq()
    node1.outboundNodes.toSeq shouldEqual Seq(2)
    val node2 = graph.getNodeById(2).get
    node2.inboundNodes.toSeq shouldEqual Seq()
    node2.outboundNodes.toSeq shouldEqual Seq()
  }

  "Add new edges in BothInOut graph" in {
    val graph = createTempGraph(StoredGraphDir.BothInOut)
    graph.addEdge(1, 2)
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toSeq shouldEqual Seq()
    node1.outboundNodes.toSeq shouldEqual Seq(2)
    val node2 = graph.getNodeById(2).get
    node2.inboundNodes.toSeq shouldEqual Seq(1)
    node2.outboundNodes.toSeq shouldEqual Seq()
  }

  "getNodeById return None if id is not in graph" in {
    val graph = createTempGraph(StoredGraphDir.BothInOut)
    graph.getNodeById(3).isDefined shouldEqual false
  }

  class asyncGraph extends SynchronizedDynamicGraph() {
    def addEdgeAsync(i: Int, j: Int) = Future {
      addEdge(i, j)
    }

    def removeEdgeAsync(i: Int, j: Int) = Future {
      removeEdge(i, j)
    }
  }

  "multiple threads add nodes/edges simultaneously" in {
    val numLoop = 20
    val graph = new asyncGraph
    val fs = (1 to numLoop) map {
      i => graph.addEdgeAsync(i, i + 1)
    }
    Await.ready(Future.sequence(fs), Duration.Inf)
    graph.nodeCount shouldEqual (numLoop + 1)
    (2 to numLoop) foreach {
      i =>
        val node = graph.getNodeById(i).get
        node.inboundNodes.toSeq shouldEqual Seq(i - 1)
        node.outboundNodes.toSeq shouldEqual Seq(i + 1)
    }
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toSeq shouldEqual Seq()
    node1.outboundNodes.toSeq shouldEqual Seq(2)
    val nodeLast = graph.getNodeById(numLoop + 1).get
    nodeLast.inboundNodes.toSeq shouldEqual Seq(numLoop)
    nodeLast.outboundNodes.toSeq shouldEqual Seq()
  }

  "multiple threads add and remove nodes/edges simultaneously" in {
    val numLoop = 20
    val graph = new asyncGraph
    val fs = (1 to numLoop) map {
      i =>
        graph.addEdgeAsync(i, i + 1) flatMap {
          case _ => graph.removeEdgeAsync(i, i + 1)
        }
    }
    Await.ready(Future.sequence(fs), Duration.Inf)

    graph.nodeCount shouldEqual (numLoop + 1)
    (1 to numLoop + 1) foreach {
      i =>
        val node = graph.getNodeById(i).get
        node.inboundNodes.toSeq shouldEqual Seq()
        node.outboundNodes.toSeq shouldEqual Seq()
    }
  }
}
