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

import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.NodeNumberer
import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import org.scalatest.WordSpec

class SharedArrayBasedDirectedGraphSpec extends WordSpec with GraphBehaviours[Node] {
    verifyGraphBuilding(SharedArrayBasedDirectedGraph.apply, sampleGraphEdges)

    "Graph constructed from file" should {
        val dir = "cassovary-core/src/test/resources/graphs"
        val reader = new AdjacencyListGraphReader(
            dir, "toy_9nodes", new NodeNumberer.IntIdentity(), _.toInt) {
            override def storedGraphDir: StoredGraphDir = StoredGraphDir.BothInOut
        }
        val graph = reader.toSharedArrayBasedDirectedGraph()
        def L = List

        val outEdges = Map[Int, Seq[Int]](0 -> L(1, 2), 1 -> L(0, 2), 2 -> L(0, 1, 3, 9),
            3 -> L(0), 9 -> L(0), 4 -> L(2, 3), 5 -> L(3, 4),
            6 -> L(1, 3), 7 -> L(0, 5), 8 -> L(2, 3))
        val inEdges = Map[Int, Seq[Int]](0 -> L(1, 2, 3, 9, 7), 1 -> L(0, 2, 6),
            2 -> L(0, 1, 4, 8), 3 -> L(2, 4, 5, 6, 8),
            4 -> L(5), 5 -> L(7), 9 -> L(2))

        verifyInOutEdges(graph, 10, outEdges, inEdges, false)
    }
}
