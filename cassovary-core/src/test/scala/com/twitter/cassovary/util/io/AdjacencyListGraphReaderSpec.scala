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
import org.scalatest.{Matchers, WordSpec}

class AdjacencyListGraphReaderSpec extends WordSpec with Matchers with GraphBehaviours[Node] {
  val directory = "cassovary-core/src/test/resources/graphs/"

  val toy6nodeMap = Map( 10 -> List(11, 12, 13), 11 -> List(12, 14), 12 -> List(14),
    13 -> List(12, 14), 14 -> List(15), 15 -> List(10, 11))

  val toy7nodeMapString = Map( "a" -> List("b"), "b" -> List("c", "d"), "c" -> List(), "d" -> List(), "" +
    "e" -> List("f"), "f" -> List("a", "b", "g"), "g" -> List())

  val toy7nodeMapLong = Map(
    10000000000000L -> List(20000000000000L),
    20000000000000L -> List(30000000000000L, 40000000000000L),
    30000000000000L -> List(),
    40000000000000L -> List(),
    50000000000000L -> List(60000000000000L),
    60000000000000L -> List(10000000000000L, 20000000000000L, 70000000000000L),
    70000000000000L -> List()
  )

  trait GraphWithoutRenumberer {
    val reader = AdjacencyListGraphReader.forIntIds(directory,
      "toy_6nodes_adj")
    val graph = reader.toSharedArrayBasedDirectedGraph()
  }

  trait ArrayBasedDynamicGraphWithoutRenumberer {
    val graph = AdjacencyListGraphReader.forIntIds(directory, "toy_6nodes_adj")
        .toArrayBasedDynamicDirectedGraph()
  }

  trait GraphWithRenumberer {
    val seqRenumberer = new SequentialNodeNumberer[Int]()
    val graph = AdjacencyListGraphReader.forIntIds(directory, "toy_6nodes_adj",
      seqRenumberer).toSharedArrayBasedDirectedGraph()
  }

  trait GraphWithStringIds {
    val seqNumberer = new SequentialNodeNumberer[String]()
    val graph = new AdjacencyListGraphReader[String](directory, "toy_7nodes_adj_StringIds",
      seqNumberer, idReader = identity).toSharedArrayBasedDirectedGraph()
  }

  trait GraphWithLongIds {
    val seqNumberer = new SequentialNodeNumberer[Long]()
    val graph = new AdjacencyListGraphReader[Long](directory, "toy_7nodes_adj_LongIds", seqNumberer,
      idReader = _.toLong).toSharedArrayBasedDirectedGraph()
  }

  "AdjacencyListReader" should {
    "provide the correct graph properties" in {
      new GraphWithoutRenumberer {
        graph.nodeCount should be(6)
        graph.edgeCount should be(11L)
        graph.maxNodeId should be(15)
      }
    }

    "contain the right nodes and edges" in {
      new GraphWithoutRenumberer {
        behave like graphEquivalentToMap(graph, toy6nodeMap)
      }
    }

    "reverse parse correctly" in {
      new GraphWithoutRenumberer {
        val node = NodeIdEdgesMaxId(10, Array(11, 12, 20))
        val nodeStr = "10 3\n11\n12\n20\n"
        reader.reverseParseNode(node) shouldEqual nodeStr
      }
    }

  }

  "AdjacencyListReader producing ArrayBasedDynamicDirectedGraph" should {
    "provide the correct graph properties" in {
      new ArrayBasedDynamicGraphWithoutRenumberer {
        graph.nodeCount should be(6)
        graph.edgeCount should be(11L)
        graph.maxNodeId should be(15)
      }
    }

    "contain the right nodes and edges" in {
      new ArrayBasedDynamicGraphWithoutRenumberer {
        behave like graphEquivalentToMap(graph, toy6nodeMap)
      }
    }
  }

  "AdjacencyListReader renumbered" when {
    "reading Int ids" should {

      "provide the correct graph properties" in {
        new GraphWithRenumberer {
          graph.nodeCount should be(6)
          graph.edgeCount should be(11L)
          graph.maxNodeId should be(5) // Zero-based; node ids are 0 thru 5.
        }
      }

      "contain the right renumbered nodes and edges" in {
        new GraphWithRenumberer {
          behave like renumberedGraphEquivalentToMap(graph, toy6nodeMap, seqRenumberer)
        }
      }
    }
    "reading String ids" should {
      "provide the correct graph properties" in {
        new GraphWithStringIds {
          graph.nodeCount should be (7)
          graph.edgeCount should be (7)
          graph.maxNodeId should be (6)
        }
      }
      "contain the right numbered nodes and edges" in {
        new GraphWithStringIds {
          behave like renumberedGraphEquivalentToMap(graph, toy7nodeMapString, seqNumberer)
        }
      }
    }
    "reading Long ids" should {
      "provide the correct graph properties" in {
        new GraphWithLongIds {
          graph.nodeCount should be (7)
          graph.edgeCount should be (7)
          graph.maxNodeId should be (6)
        }
      }
      "contain the right numbered nodes and edges" in {
        new GraphWithLongIds {
          behave like renumberedGraphEquivalentToMap(graph, toy7nodeMapLong, seqNumberer)
        }
      }
    }
  }
}
