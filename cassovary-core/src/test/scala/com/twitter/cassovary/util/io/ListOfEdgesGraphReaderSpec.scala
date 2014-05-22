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

import com.twitter.cassovary.graph.GraphBehaviours
import com.twitter.cassovary.util.SequentialNodeNumberer
import java.util.concurrent.Executors
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class ListOfEdgesGraphReaderSpec extends WordSpec with GraphBehaviours with ShouldMatchers {

  val intGraphMap = Map(1 -> List(2, 3), 2 -> List(3), 3 -> List(4), 4 -> List(1))

  val stringGraphMap = Map("a" -> List("b"), "b" -> List("c"), "c" -> List("d"), "d" -> List("e"),
    "e" -> List("a"))

  private val DIRECTORY: String = "cassovary-core/src/test/resources/graphs/"

  trait GraphWithIntIds {
    val graph = ListOfEdgesGraphReader.forIntIds(DIRECTORY, "toy_list5edges",
      Executors.newFixedThreadPool(2)).toArrayBasedDirectedGraph()
  }

  trait GraphWithStringIds {
    val seqNumberer = new SequentialNodeNumberer[String]()
    val graph = new ListOfEdgesGraphReader(DIRECTORY, "toy_6nodes_list", seqNumberer,
      idReader = identity){
      override val executorService = Executors.newFixedThreadPool(2)
    }.toSharedArrayBasedDirectedGraph()
  }

  "ListOfEdgesReader" when {
    "using Int ids" should {

      "provide the correct graph properties" in {
        new GraphWithIntIds {
          graph.nodeCount should be (4)
          graph.edgeCount should be (5L)
          graph.maxNodeId should be (4)
        }
      }

      "contain the right nodes and edges" in {
        new GraphWithIntIds {
          behave like graphEquivalentToMap(graph, intGraphMap)
        }
      }
    }
    "using String ids" in {
      new GraphWithStringIds {
        graph.nodeCount should be (6)
        graph.edgeCount should be (5L)
        graph.maxNodeId should be (5)
      }
    }
  }
}
