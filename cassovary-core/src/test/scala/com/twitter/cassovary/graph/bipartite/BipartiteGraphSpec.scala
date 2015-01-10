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

import com.twitter.cassovary.graph.GraphDir
import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import scala.collection.mutable

class BipartiteGraphSpec extends WordSpec with ShouldMatchers {

  var graph: BipartiteGraph = _
  var leftNodes: Array[BipartiteNode] = _
  var rightNodes: Array[BipartiteNode] = _

  def bipartiteExampleSingleSide() = {
   /*
   lN -> 1 to 5
   rN -> 4,8,5,10,123,0
   1 --> i:(), o:()
   2 --> i: (), o: (5,10)
   3 --> i: (), o: ()
   4 --> i: (), o: (14)
   5 --> i: (), o: (5,10,8)
   */

    leftNodes = new Array[BipartiteNode](6)

    var inBounds: Array[Int] = Array()
    var outBounds: Array[Int] = Array()
    createLeftNode(1, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array(5, 10)
    createLeftNode(2, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array()
    createLeftNode(3, inBounds, outBounds)


    inBounds = Array()
    outBounds = Array(14)
    createLeftNode(4, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array(5, 10, 8)
    createLeftNode(5, inBounds, outBounds)

    rightNodes = new Array[BipartiteNode](124)
    inBounds = Array(4)
    outBounds = Array()
    createRightNode(14, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array()
    createRightNode(4, inBounds, outBounds)

    inBounds = Array(2, 5)
    outBounds = Array()
    createRightNode(5, inBounds, outBounds)

    inBounds = Array(5)
    outBounds = Array()
    createRightNode(8, inBounds, outBounds)

    inBounds = Array(2, 5)
    outBounds = Array()
    createRightNode(10, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array()
    createRightNode(123, inBounds, outBounds)

    val leftSide = BipartiteSide(leftNodes, 5, 6)
    val rightSide = BipartiteSide(rightNodes, 6, 0)

    graph = new BipartiteGraph(leftSide, rightSide, BipartiteGraphDir.LeftToRight)
  }

  def bipartiteExampleDoubleSide() = {
   /*
   lN -> 1 to 5
   rN -> 4,8,5,10,123,0
   1 --> i:(4,5,123,10), o:()
   2 --> i: (), o: (5,10)
   3 --> i: (), o: ()
   4 --> i: (14), o: (14)
   5 --> i: (4,10), o: (5,10,8)
   */

    leftNodes = new Array[BipartiteNode](6)

    var inBounds: Array[Int] = Array(4, 5, 123, 10)
    var outBounds: Array[Int] = Array()
    createLeftNode(1, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array(5, 10)
    createLeftNode(2, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array()
    createLeftNode(3, inBounds, outBounds)


    inBounds = Array(14)
    outBounds = Array(14)
    createLeftNode(4, inBounds, outBounds)

    inBounds = Array(4, 10)
    outBounds = Array(5, 10, 8)
    createLeftNode(5, inBounds, outBounds)

    rightNodes = new Array[BipartiteNode](124)
    inBounds = Array(4)
    outBounds = Array(4)
    createRightNode(14, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array(1, 5)
    createRightNode(4, inBounds, outBounds)

    inBounds = Array(2, 5)
    outBounds = Array(1)
    createRightNode(5, inBounds, outBounds)

    inBounds = Array(5)
    outBounds = Array()
    createRightNode(8, inBounds, outBounds)

    inBounds = Array(2, 5)
    outBounds = Array(1, 5)
    createRightNode(10, inBounds, outBounds)

    inBounds = Array()
    outBounds = Array(1)
    createRightNode(123, inBounds, outBounds)

    val leftSide = BipartiteSide(leftNodes, 5, 6)
    val rightSide = BipartiteSide(rightNodes, 6, 7)

    graph = new BipartiteGraph(leftSide, rightSide, BipartiteGraphDir.Both)
  }

  "Bipartite Graph Single Side Input" should {
    "have loaded correct number of nodes and edges" in {
      bipartiteExampleSingleSide()
      graph.leftNodeCount shouldEqual 5
      graph.leftOutEdgeCount shouldEqual 6
      graph.rightNodeCount shouldEqual 6
      graph.rightOutEdgeCount shouldEqual 0
    }

    "have the right input to output node mapping" in {
      bipartiteExampleSingleSide()
      val leftNbrSet = mutable.Set[Int]()
      (-5 to -1).toArray foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode shouldEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode shouldEqual false
          leftNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir).toList shouldEqual Nil
      }

      leftNbrSet shouldEqual Set(14, 5, 8, 10)

      val rightNbrSet = mutable.Set[Int]()
      Array(14, 4, 5, 8, 10, 123) foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode shouldEqual false
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode shouldEqual true
          rightNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.OutDir).toList shouldEqual Nil
      }
      rightNbrSet shouldEqual Set(2, 4, 5)

      graph.getNodeById(0) shouldEqual None
      graph.getNodeById(-10) shouldEqual None
      graph.getNodeById(11) shouldEqual None
      graph.getNodeById(150) shouldEqual None
    }
  }

  "Bipartite Graph Double Sides input " should {
    "have loaded correct number of nodes and edges" in {
      bipartiteExampleDoubleSide()
      graph.leftNodeCount shouldEqual 5
      graph.leftOutEdgeCount shouldEqual 6
      graph.rightNodeCount shouldEqual 6
      graph.rightOutEdgeCount shouldEqual 7
    }

    "have the right input to output node mapping" in {
      bipartiteExampleDoubleSide()
      val leftInNbrSet = mutable.Set[Int]()
      val leftOutNbrSet = mutable.Set[Int]()
      (-5 to -1).toArray foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode shouldEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode shouldEqual false
          leftOutNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode shouldEqual false
          leftInNbrSet += nbrNode.id
        }
      }
      leftOutNbrSet shouldEqual Set(14, 5, 8, 10)
      leftInNbrSet shouldEqual Set(14, 4, 5, 10, 123)

      val rightInNbrSet = mutable.Set[Int]()
      val rightOutNbrSet = mutable.Set[Int]()
      Array(14, 4, 5, 8, 10, 123) foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode shouldEqual false
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode shouldEqual true
          rightInNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode shouldEqual true
          rightOutNbrSet += nbrNode.id
        }
      }
      rightOutNbrSet shouldEqual Set(1, 4, 5)
      rightInNbrSet shouldEqual Set(2, 4, 5)
    }
  }

  private def createLeftNode(id: Int, inNodes: Array[Int], outNodes: Array[Int]) {
    val node = new LeftNode(id, inNodes, outNodes)
    leftNodes(id) = node
  }

  private def createRightNode(id: Int, inNodes: Array[Int], outNodes: Array[Int]) {
    val node = new RightNode(id, inNodes, outNodes)
    rightNodes(id) = node
  }
}
