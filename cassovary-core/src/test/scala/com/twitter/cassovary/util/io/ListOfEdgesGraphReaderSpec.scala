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
package com.twitter.cassovary.util.io

import org.specs.Specification
import com.twitter.cassovary.graph.DirectedGraph
import java.util.concurrent.Executors

class ListOfEdgesGraphReaderSpec extends Specification with GraphMapEquality {

  val nodeMap = Map(1 -> List(2, 3), 2 -> List(3), 3 -> List(4), 4 -> List(1))

  var graph: DirectedGraph = _

  "ListOfEdgesReader" should {

    doBefore {
      // Example using 2 threads to read in the graph
      graph = new ListOfEdgesGraphReader("cassovary-core/src/test/resources/graphs/", "toy_list5edges") {
        override val executorService = Executors.newFixedThreadPool(2)
      }.toArrayBasedDirectedGraph()
    }

    "provide the correct graph properties" in {
      graph.nodeCount mustBe 4
      graph.edgeCount mustBe 5L
      graph.maxNodeId mustBe 4
    }

    "contain the right nodes and edges" in {
      nodeMapEquals(graph, nodeMap)
    }

  }
}
