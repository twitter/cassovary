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
package com.twitter.cassovary.graph.bipartite

import com.twitter.cassovary.graph.{GraphDir, TestGraphs}
import org.scalatest.{Matchers, WordSpec}
import scala.collection.mutable

class BipartiteGraphSpec extends WordSpec with Matchers {

   "Bipartite Graph Single Side Input" should {
    val graph = TestGraphs.bipartiteGraphSingleSide
    "have loaded correct number of nodes and edges" in {
      graph.leftNodeCount shouldEqual 5
      graph.leftOutEdgeCount shouldEqual 6
      graph.rightNodeCount shouldEqual 6
      graph.rightOutEdgeCount shouldEqual 0
    }

    "have the right input to output node mapping" in {
      val leftNbrSet = mutable.Set[Int]()
      (-5 to -1).toArray foreach { id =>
        val node = graph.getNodeById(id).get
        node.isLeftNode shouldEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual false
          leftNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir).toSeq shouldEqual Seq()
      }

      leftNbrSet shouldEqual Set(14, 5, 8, 10)

      val rightNbrSet = mutable.Set[Int]()
      Array(14, 4, 5, 8, 10, 123) foreach { id =>
        val node = graph.getNodeById(id).get
        node.isLeftNode shouldEqual false
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual true
          rightNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.OutDir).toSeq shouldEqual Seq()
      }
      rightNbrSet shouldEqual Set(-2, -4, -5)

      graph.getNodeById(0) shouldEqual None
      graph.getNodeById(-10) shouldEqual None
      graph.getNodeById(11) shouldEqual None
      graph.getNodeById(150) shouldEqual None
    }
  }

  "Bipartite Graph Double Sides input " should {
    val graph = TestGraphs.bipartiteGraphDoubleSide
    "have loaded correct number of nodes and edges" in {
      graph.leftNodeCount shouldEqual 5
      graph.leftOutEdgeCount shouldEqual 6
      graph.rightNodeCount shouldEqual 6
      graph.rightOutEdgeCount shouldEqual 7
    }

    "have the right input to output node mapping" in {
      val leftInNbrSet = mutable.Set[Int]()
      val leftOutNbrSet = mutable.Set[Int]()
      (-5 to -1).toArray foreach { id =>
        val node = graph.getNodeById(id).get
        node.isLeftNode shouldEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual false
          leftOutNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual false
          leftInNbrSet += nbrNode.id
        }
      }
      leftOutNbrSet shouldEqual Set(14, 5, 8, 10)
      leftInNbrSet shouldEqual Set(14, 4, 5, 10, 123)

      val rightInNbrSet = mutable.Set[Int]()
      val rightOutNbrSet = mutable.Set[Int]()
      Array(14, 4, 5, 8, 10, 123) foreach { id =>
        val node = graph.getNodeById(id).get
        node.isLeftNode shouldEqual false
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual true
          rightInNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual true
          rightOutNbrSet += nbrNode.id
        }
      }
      rightOutNbrSet shouldEqual Set(-1, -4, -5)
      rightInNbrSet shouldEqual Set(-2, -4, -5)
    }
  }

  "Bipartite Graph Unique Node Ids Input" should {
    val graph = TestGraphs.bipartiteGraphWithUniqueNodeIds
    "have loaded correct number of nodes and edges" in {
      graph.leftNodeCount shouldEqual 3
      graph.leftOutEdgeCount shouldEqual 4
      graph.rightNodeCount shouldEqual 3
      graph.rightOutEdgeCount shouldEqual 5
    }

    "have the right input to output node mapping" in {
      val leftInNbrSet = mutable.Set[Int]()
      val leftOutNbrSet = mutable.Set[Int]()
      Array(2, 4, 6) foreach { id =>
        val node = graph.getNodeById(id).get
        node.isLeftNode shouldEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual false
          leftOutNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual false
          leftInNbrSet += nbrNode.id
        }
      }
      leftInNbrSet shouldEqual Set(1, 3, 5)
      leftOutNbrSet shouldEqual Set(1, 3, 5)

      val rightInNbrSet = mutable.Set[Int]()
      val rightOutNbrSet = mutable.Set[Int]()
      Array(1, 3, 5) foreach { id =>
        val node = graph.getNodeById(id).get
        node.isLeftNode shouldEqual false
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual true
          rightOutNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get
          nbrNode.isLeftNode shouldEqual true
          rightInNbrSet += nbrNode.id
        }
      }
      rightInNbrSet shouldEqual Set(2, 4, 6)
      rightOutNbrSet shouldEqual Set(2, 4, 6)
    }
  }
}
