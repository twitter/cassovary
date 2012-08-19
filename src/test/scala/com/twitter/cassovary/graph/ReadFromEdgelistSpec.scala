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
import StoredGraphDir.StoredGraphDir

class ReadFromEdgelistSpec extends Specification {
  var graph: ArrayBasedDirectedGraph = _
  val edgelistPath = "src/test/resources/graphs/ExampleGraph.edgelist"

  def makeGraph(dir: StoredGraphDir) = 
    ReadFromEdgelist.arrayBasedDirectedGraph(edgelistPath, dir)

  val smallGraph = beforeContext {
    graph = makeGraph(StoredGraphDir.BothInOut)
  }

  val smallGraphOutOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyOut)
  }

  val smallGraphInOnly = beforeContext {
    graph = makeGraph(StoredGraphDir.OnlyIn)
  }

  "the graph" definedAs smallGraph should {
    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 5).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 5
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 5L
    }
  }

  "graph containing only out edges" definedAs smallGraphOutOnly should {
    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 5).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 5
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 5L
    }
  }

  "graph containing only in edges" definedAs smallGraphInOnly should {
    "iterate over all nodes" in {
      graph must DeepEqualsNodeIterable((1 to 5).flatMap(getNode(_)))
    }

    "provide the correct node count" in {
      graph.nodeCount mustBe 5
    }

    "provide the correct edge count" in {
      graph.edgeCount mustBe 5L
    }

    "provide the correct max node id" in {
      graph.maxNodeId mustBe 5
    }
  }

  def getNode(id: Int): Option[Node] = graph.getNodeById(id)
}
