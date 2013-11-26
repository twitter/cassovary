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
package com.twitter.cassovary.graph

import org.specs.Specification
import StoredGraphDir._

class SynchronizedDynamicGraphSpec extends Specification {
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

  "multipe threads add nodes/edge simultaneously" in {
    import scala.actors.Actor
    import scala.actors.Actor._

    val graph = new SynchronizedDynamicGraph()

    val numActors = 5
    val writers = (0 until numActors) map { n  =>
      actor {
        loop { receive {
          case (i: Int, j: Int) => graph.addEdge(i, j)
          case _: String => reply
        }}
      }
    }

    val numLoop = 15
    (1 to numLoop) foreach { i =>
      val src = i
      val dest = i + 1
      val pair = (src, dest)
      val n = i % numActors
      writers(n) ! pair
    }

    (0 until numActors) foreach { writers(_) !? "stop" }

    graph.nodeCount mustEqual (numLoop + 1)
    (2 to numLoop) foreach { i =>
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

  "multipe threads add/remove edge simultaneously" in {
    import scala.actors.Actor
    import scala.actors.Actor._

    val graph = new SynchronizedDynamicGraph()

    val writer = actor {
      loop { receive {
        case (i: Int, j: Int) => {
          graph.addEdge(i, j)
        }
        case _: String => reply
      }}
    }
    val remover = actor {
      loop { receive {
        case (i: Int, j: Int) => {
          graph.removeEdge(i, j)
        }
        case _: String => reply
      }}
    }

    val numLoop = 10
    (1 to numLoop) foreach { i =>
      val src = i
      val dest = i + 1
      val pair = (src, dest)
      writer ! pair
    }
    writer !? "stop"

    (1 to numLoop) foreach { i =>
      val src = i
      val dest = i + 1
      val pair = (src, dest)
      remover ! pair
    }
    remover !? "stop"

    graph.nodeCount mustEqual 11
    (1 to numLoop + 1) foreach { i =>
      val node = graph.getNodeById(i).get
      node.inboundNodes.toList mustEqual List()
      node.outboundNodes.toList mustEqual List()
    }
  }
}
