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

import com.twitter.cassovary.graph.{NodeIdEdgesMaxId, GraphBehaviours, Node}
import com.twitter.cassovary.util.SequentialNodeNumberer
import java.util.concurrent.Executors
import org.scalatest.{Matchers, WordSpec}

class ListOfEdgesGraphReaderSpec extends WordSpec with GraphBehaviours[Node] with Matchers {

  val intGraphMap = Map(1 -> List(2, 3), 2 -> List(3), 3 -> List(4), 4 -> List(1))

  val stringGraphMap = Map("a" -> List("b"), "b" -> List("c"), "c" -> List("d"), "d" -> List("e"),
    "e" -> List("f"), "f" -> List("a"))

  val longGraphMap = Map(
    100000000000L -> List(200000000000L),
    200000000000L -> List(300000000000L),
    300000000000L -> List(400000000000L),
    400000000000L -> List(500000000000L),
    500000000000L -> List(600000000000L),
    600000000000L -> List(100000000000L)
  )

  private val directory: String = "cassovary-core/src/test/resources/graphs/"

  trait GraphWithIntIds {
    val reader = ListOfEdgesGraphReader.forIntIds(directory, "toy_list5edges",
      Executors.newFixedThreadPool(2))
    val graph = reader.toArrayBasedDirectedGraph()
  }

  trait GraphWithStringIds {
    val seqNumberer = new SequentialNodeNumberer[String]()
    val graph = new ListOfEdgesGraphReader(directory, "toy_6nodes_list_StringIds", seqNumberer,
      idReader = identity){
      override val executorService = Executors.newFixedThreadPool(2)
    }.toSharedArrayBasedDirectedGraph()
  }

  trait GraphWithLongIds {
    val seqNumberer = new SequentialNodeNumberer[Long]()
    val graph = new ListOfEdgesGraphReader(directory, "toy_6nodes_list_LongIds", seqNumberer,
      idReader = _.toLong){
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

      "reverse parse a node correctly" in {
        new GraphWithIntIds {
          val node = NodeIdEdgesMaxId(10, Array(11, 12, 13))
          val nodeStr = "10 11\n10 12\n10 13\n"
          reader.reverseParseNode(node) shouldEqual nodeStr
        }
      }
    }

    "using String ids" should {
      "provide the correct graph properties" in {
        new GraphWithStringIds {
          graph.nodeCount should be(6)
          graph.edgeCount should be(6L)
          graph.maxNodeId should be(5)
        }
      }
      "contain the right nodes and edges" in {
        new GraphWithStringIds {
          behave like renumberedGraphEquivalentToMap(graph, stringGraphMap, seqNumberer)
        }
      }
    }
    "using Long ids" should {
      "provide the correct graph properties" in {
        new GraphWithStringIds {
          graph.nodeCount should be(6)
          graph.edgeCount should be(6L)
          graph.maxNodeId should be(5)
        }
      }
      "contain the right nodes and edges" in {
        new GraphWithLongIds {
          behave like renumberedGraphEquivalentToMap(graph, longGraphMap, seqNumberer)
        }
      }
    }
  }
}
