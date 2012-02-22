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
package com.twitter.cassovary.graph.bipartite

import com.twitter.cassovary.graph.bipartite.BipartiteGraphDir._
import com.twitter.cassovary.graph.GraphDir
import com.twitter.cassovary.graph.GraphDir._
import org.specs.Specification
import scala.collection.mutable

class BipartiteGraphSpec extends Specification {

  var graph: BipartiteGraph = _
  var leftNodes: Array[BipartiteNode] = _
  var rightNodes: Array[BipartiteNode] = _

  val bipartiteExampleSingleSide = beforeContext {
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

  val bipartiteExampleDoubleSide = beforeContext {
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

  "Bipartite Graph Single Side Input" definedAs bipartiteExampleSingleSide should{
    "have loaded correct number of nodes and edges" in {
      graph.leftNodeCount mustEqual 5
      graph.leftOutEdgeCount mustEqual 6
      graph.rightNodeCount mustEqual 6
      graph.rightOutEdgeCount mustEqual 0
    }

    "have the right input to output node mapping" in {
      val leftNbrSet = mutable.Set[Int]()
      (-5 to -1).toArray foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode mustEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode mustEqual false
          leftNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir).toList mustEqual Nil
      }

      leftNbrSet mustEqual Set(14, 5, 8, 10)

      val rightNbrSet = mutable.Set[Int]()
      Array(14, 4, 5, 8, 10, 123) foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode mustEqual false
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode mustEqual true
          rightNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.OutDir).toList mustEqual Nil
      }
      rightNbrSet mustEqual Set(2, 4, 5)

      graph.getNodeById(0) mustEqual None
      graph.getNodeById(-10) mustEqual None
      graph.getNodeById(11) mustEqual None
      graph.getNodeById(150) mustEqual None
    }
  }

  "Bipartite Graph Double Sides input " definedAs bipartiteExampleDoubleSide should{
    "have loaded correct number of nodes and edges" in {
      graph.leftNodeCount mustEqual 5
      graph.leftOutEdgeCount mustEqual 6
      graph.rightNodeCount mustEqual 6
      graph.rightOutEdgeCount mustEqual 7
    }

    "have the right input to output node mapping" in {
      val leftInNbrSet = mutable.Set[Int]()
      val leftOutNbrSet = mutable.Set[Int]()
      (-5 to -1).toArray foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode mustEqual true
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode mustEqual false
          leftOutNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode mustEqual false
          leftInNbrSet += nbrNode.id
        }
      }
      leftOutNbrSet mustEqual Set(14, 5, 8, 10)
      leftInNbrSet mustEqual Set(14, 4, 5, 10, 123)

      val rightInNbrSet = mutable.Set[Int]()
      val rightOutNbrSet = mutable.Set[Int]()
      Array(14, 4, 5, 8, 10, 123) foreach { id =>
        val node = graph.getNodeById(id).get.asInstanceOf[BipartiteNode]
        node.isLeftNode mustEqual false
        node.neighborIds(GraphDir.InDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode mustEqual true
          rightInNbrSet += nbrNode.id
        }
        node.neighborIds(GraphDir.OutDir) foreach { nbrId =>
          val nbrNode = graph.getNodeById(nbrId).get.asInstanceOf[BipartiteNode]
          nbrNode.isLeftNode mustEqual true
          rightOutNbrSet += nbrNode.id
        }
      }
      rightOutNbrSet mustEqual Set(1, 4, 5)
      rightInNbrSet mustEqual Set(2, 4, 5)
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
