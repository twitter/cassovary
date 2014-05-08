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
import org.specs.Specification
import StoredGraphDir._
import scala.concurrent.{Await, ExecutionContext, Future}
import scala.concurrent.duration.Duration

class SynchronizedDynamicGraphSpec extends Specification {
  implicit val ecctxt = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(4))

  "Add new nodes" in {
    val graph = new SynchronizedDynamicGraph()
    graph.nodeCount mustEqual 0
    graph.edgeCount mustEqual 0
    val node1 = graph.getOrCreateNode(1)
    node1.id mustEqual 1
    graph.nodeCount mustEqual 1
    graph.edgeCount mustEqual 0

    val node2 = graph.getOrCreateNode(2)
    node2.id mustEqual 2
    graph.nodeCount mustEqual 2
    graph.edgeCount mustEqual 0
  }

  def createTempGraph(storedGraphDir: StoredGraphDir) = {
    val graph = new SynchronizedDynamicGraph(storedGraphDir)
    graph.getOrCreateNode(1)
    graph.getOrCreateNode(2)
    graph
  }

  "Add new edges in OnlyIn graph" in {
    val graph = createTempGraph(StoredGraphDir.OnlyIn)
    graph.addEdge(1, 2)
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toList mustEqual List()
    node1.outboundNodes.toList mustEqual List()
    val node2 = graph.getNodeById(2).get
    node2.inboundNodes.toList mustEqual List(1)
    node2.outboundNodes.toList mustEqual List()
  }

  "Add new edges in OnlyOut graph" in {
    val graph = createTempGraph(StoredGraphDir.OnlyOut)
    graph.addEdge(1, 2)
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toList mustEqual List()
    node1.outboundNodes.toList mustEqual List(2)
    val node2 = graph.getNodeById(2).get
    node2.inboundNodes.toList mustEqual List()
    node2.outboundNodes.toList mustEqual List()
  }

  "Add new edges in BothInOut graph" in {
    val graph = createTempGraph(StoredGraphDir.BothInOut)
    graph.addEdge(1, 2)
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toList mustEqual List()
    node1.outboundNodes.toList mustEqual List(2)
    val node2 = graph.getNodeById(2).get
    node2.inboundNodes.toList mustEqual List(1)
    node2.outboundNodes.toList mustEqual List()
  }

  "getNodeById return None if id is not in graph" in {
    val graph = createTempGraph(StoredGraphDir.BothInOut)
    graph.getNodeById(3).isDefined mustEqual false
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
    graph.nodeCount mustEqual (numLoop + 1)
    (2 to numLoop) foreach {
      i =>
        val node = graph.getNodeById(i).get
        node.inboundNodes.toList mustEqual List(i - 1)
        node.outboundNodes.toList mustEqual List(i + 1)
    }
    val node1 = graph.getNodeById(1).get
    node1.inboundNodes.toList mustEqual List()
    node1.outboundNodes.toList mustEqual List(2)
    val nodeLast = graph.getNodeById(numLoop + 1).get
    nodeLast.inboundNodes.toList mustEqual List(numLoop)
    nodeLast.outboundNodes.toList mustEqual List()
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

    graph.nodeCount mustEqual (numLoop + 1)
    (1 to numLoop + 1) foreach {
      i =>
        val node = graph.getNodeById(i).get
        node.inboundNodes.toList mustEqual List()
        node.outboundNodes.toList mustEqual List()
    }
  }
}
