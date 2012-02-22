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

import org.specs.mock.Mockito
import org.specs.Specification
import scala.util.Random

class TraverserSpec extends Specification with Mockito {

  class TestIter extends Iterator[Int] {
    var i = 0
    def next = { i += 10 ; i }
    def hasNext = (i < 40)
  }

  "BoundedIterator" should {
    "satisfy a limit" in {
      val biter = new TestIter with BoundedIterator[Int] {
        lazy val maxSteps = 3L
      }
      biter.toSeq.toList mustEqual List(10, 20, 30)
    }
   }

  "RandomTraverser" should {
    val graph = TestGraphs.g6
    def getNode(id: Int) = graph.getNodeById(id).get

    "yield the home node at first step and go to a neighbor when resetprob equals 0" in {
      val resetProb = 0.0
      val dir = GraphDir.OutDir
      val randomTraverser = new RandomTraverser(
        graph, dir, Seq(10), resetProb, None, false, new Random, None, false)
      val homeNode = getNode(10)
      randomTraverser.next must_== homeNode
      var curr = homeNode
      for (i <- 1 to 10) {
        val next = randomTraverser.next
        next must_!= curr
        curr.isNeighbor(dir, next.id) must_== true
        curr = next
      }
    }

    "test onlyOnce option on graph3, when it is set to true, walk should reset to homenode" +
        "when visit the same node twice, even when resetprob equals 0, " +
        "if option not set, walk will run into infinite loop" in {
      val mockRandom = mock[Random]
      //mock random always returns the last element
      mockRandom.nextDouble() returns 1.0
      val graph3 = TestGraphs.g3
      def getNode(id: Int) = graph3.getNodeById(id).get

      val resetProb = 0.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(
            graph3, dir, Seq(10), resetProb, None, true, mockRandom, None, false)
        val homeNode = getNode(10)
        mockRandom.nextInt(1) returns 0
        randomTraverser.next must_== homeNode
        mockRandom.nextInt(2) returns 1
        var next = randomTraverser.next
        next.id mustEqual 12
        next = randomTraverser.next
        next.id mustEqual 11
        //the set of edges are (10->11), (10->12), (11->12), (12->11)
        //if onceOnly is not set, we will visit 12
        //but because 12 has been seen, we reset to homenode
        next = randomTraverser.next
        next.id mustEqual 10
      }
    }

    "test onlyOnce option on graph6, when it is set to true, walk should reset to homenode " +
        "when visit the same node twice, even when resetprob equals 0" in {
      val mockRandom = mock[Random]
      //mock random always returns the last element
      mockRandom.nextDouble() returns 1.0

      val resetProb = 0.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(graph, dir, Seq(12),
            resetProb, None, true, mockRandom, None, false)
        val homeNode = getNode(12)
        mockRandom.nextInt(1) returns 0
        randomTraverser.next must_== homeNode
        var next = randomTraverser.next
        next.id mustEqual 14
        next = randomTraverser.next
        next.id mustEqual 15
        //node 15 has 2 neighbors (10,11), we visit 11
        mockRandom.nextInt(2) returns 1
        next = randomTraverser.next
        next.id mustEqual 11
        //11 has 2 neighbors (12,14), our mockRandom will pick 14
        //but 14 has been visited before, reset back to homenode(12)
        next = randomTraverser.next
        next.id mustEqual 12
      }
    }

    "test onlyOnce option on graph6, when it is set to true, walk should reset to homenode when " +
        "visit the same node twice, even when resetprob equals 0" in {
      val mockRandom = mock[Random]
      //mock random always returns the last element
      mockRandom.nextDouble() returns 1.0

      val resetProb = 0.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(graph, dir, Seq(12),
          resetProb, None, true, mockRandom, None, false)
        val homeNode = getNode(12)
        mockRandom.nextInt(1) returns 0
        randomTraverser.next must_== homeNode
        var next = randomTraverser.next
        next.id mustEqual 14
        next = randomTraverser.next
        next.id mustEqual 15
        //node 15 has 2 neighbors (10,11), we visit 11
        mockRandom.nextInt(2) returns 1
        next = randomTraverser.next
        next.id mustEqual 11
        //11 has 2 neighbors (12,14), our mockRandom will pick 14
        //but 14 has been visited before, reset back to homenode(12)
        next = randomTraverser.next
        next.id mustEqual 12
      }
    }

    "always go to the home node when resetprob equals 1" in {
      val resetProb = 1.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(graph, dir, Seq(10), resetProb,
          None, false, new Random, None, false)
        val homeNode = getNode(10)
        randomTraverser.next must_== homeNode
        var curr = homeNode
        for (i <- 1 to 10) {
          randomTraverser.next must_== curr
        }
      }
    }
  }

  "BreadthFirstTraverser" should {
    val graph = TestGraphs.g6

    "yield all nodes in BFS order in non-unique id walk" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(10),
        Some(5), None, None, 10L, false) with BoundedIterator[Node] {
        lazy val maxSteps = 10L
      }
      val ids = bfs.toSeq.map { _.id }.toList
      ids must_== List(11, 12, 13, 12, 14, 14, 12, 14, 14, 15)
      bfs.depth(10) mustEqual Some(0)
      List(11, 12, 13) foreach { bfs.depth(_) mustEqual Some(1) }
      List(14) foreach { bfs.depth(_) mustEqual Some(2) }
      List(15) foreach { bfs.depth(_) mustEqual Some(3) }
      bfs.depth(16) mustEqual None
    }

    "yield all nodes in BFS order in unique id walk" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(10), Some(5),
          None, None, 10L, true) with BoundedIterator[Node] {
        lazy val maxSteps = 10L
      }
      val ids = bfs.toSeq.map { _.id }.toList
      ids must_== List(11, 12, 13, 14, 15)
      bfs.depth(10) mustEqual Some(0)
      List(11, 12, 13) foreach { bfs.depth(_) mustEqual Some(1) }
      List(14) foreach { bfs.depth(_) mustEqual Some(2) }
      List(15) foreach { bfs.depth(_) mustEqual Some(3) }
      bfs.depth(16) mustEqual None
    }

    "yield all nodes in BFS order walk with constraint numOfFriendsThresh" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(15), Some(5), None,
          Some(2), 10L, false) with BoundedIterator[Node] {
        lazy val maxSteps = 10L
      }
      val ids = bfs.toSeq.map { _.id }.toList
      ids must_== List(10, 11, 12, 14, 14, 15, 15, 10, 11, 10)
      bfs.depth(15) mustEqual Some(0)
      List(10, 11) foreach { bfs.depth(_) mustEqual Some(1) }
      List(12, 14) foreach { bfs.depth(_) mustEqual Some(2) }
      List(13, 16) foreach { bfs.depth(_) mustEqual None }
    }
  }

  "DepthFirstTraverser" should {
    val graph = TestGraphs.g6

    "yield all nodes in DFS order in expected order in unique id walk (outDir)" in {
      val dir = GraphDir.OutDir
      val trav = new DepthFirstTraverser(graph, dir, Seq(10), true)
      val ids = trav.toSeq.map { _.id }.toList
      ids must_== List(10, 11, 12, 14, 15, 13)

      trav.distance(10) mustEqual Some(0)
      trav.distance(11) mustEqual Some(1)
      trav.distance(12) mustEqual Some(2)
      trav.distance(14) mustEqual Some(3)
      trav.distance(15) mustEqual Some(4)
      trav.distance(13) mustEqual Some(1)
    }

    "yield all nodes in DFS order in expected order in non-unique id walk (outDir)" in {
      val dir = GraphDir.OutDir
      val trav = new DepthFirstTraverser(graph, dir, Seq(10), false) with BoundedIterator[Node] {
        lazy val maxSteps = 7L
      }
      val ids = trav.toSeq.map { _.id }.toList
      ids must_== List(10, 11, 12, 14, 15, 10, 11)

      trav.distance(10) mustEqual Some(0)
      trav.distance(11) mustEqual Some(1)
      trav.distance(12) mustEqual Some(2)
    }

    "yield all nodes in DFS order in expected order in unique id walk (inDir)" in {
      val dir = GraphDir.InDir
      val trav = new DepthFirstTraverser(graph, dir, Seq(10), true) with BoundedIterator[Node] {
        lazy val maxSteps = 11L
      }
      val ids = trav.toSeq.map { _.id }.toList
      ids must_== List(10, 15, 14, 11, 12, 13)

      trav.distance(10) mustEqual Some(0)
      trav.distance(15) mustEqual Some(1)
      trav.distance(14) mustEqual Some(2)
      trav.distance(11) mustEqual Some(3)
      trav.distance(12) mustEqual Some(3)
      trav.distance(13) mustEqual Some(4)
    }
  }
}
