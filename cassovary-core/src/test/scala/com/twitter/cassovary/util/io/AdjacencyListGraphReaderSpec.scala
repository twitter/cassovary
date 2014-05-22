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

class AdjacencyListGraphReaderSpec extends WordSpec with ShouldMatchers with GraphBehaviours {
  val DIRECTORY = "cassovary-core/src/test/resources/graphs/"

  val toy6nodeMap = Map( 10 -> List(11, 12, 13), 11 -> List(12, 14), 12 -> List(14),
    13 -> List(12, 14), 14 -> List(15), 15 -> List(10, 11))

  val toy7nodeMap = Map( "a" -> List("b"), "b" -> List("c", "d"), "c" -> List(), "d" -> List(), "" +
    "e" -> List("f"), "f" -> List("a", "b", "g"), "g" -> List())

  trait GraphWithoutRenumberer {
    val graph = AdjacencyListGraphReader.forIntIds(DIRECTORY, "toy_6nodes_adj",
      Executors.newFixedThreadPool(2)).toSharedArrayBasedDirectedGraph()
  }

  trait GraphWithRenumberer {
    val seqRenumberer = new SequentialNodeNumberer[Int]()
    val graph = AdjacencyListGraphReader.forIntIds(DIRECTORY, "toy_6nodes_adj",
      Executors.newFixedThreadPool(2), seqRenumberer).toSharedArrayBasedDirectedGraph()
  }

  trait GraphWithStringIds {
    val seqNumberer = new SequentialNodeNumberer[String]()
    val graph = new AdjacencyListGraphReader[String](DIRECTORY, "toy_7nodes_adj_StringIds", seqNumberer,
      idReader = identity, separator = " ", quotationMark =  "\""){
      override val executorService = Executors.newFixedThreadPool(2)
    }.toSharedArrayBasedDirectedGraph()
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
      new GraphWithoutRenumberer {0
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
          behave like renumberedGraphEquivalentToMap(graph, toy7nodeMap, seqNumberer)
        }
      }
    }
  }
}
