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

import com.twitter.cassovary.graph.bipartite.BipartiteNode
import org.mockito.Mockito._
import org.scalatest.{Matchers, WordSpec}
import org.scalatest.mock.MockitoSugar

import scala.util.Random

class TraverserSpec extends WordSpec with MockitoSugar with Matchers {

  class TestIter extends Iterator[Int] {
    var i = 0
    def next() = { i += 10 ; i }
    def hasNext = i < 40
  }

  "ShortestPathTraverser" should {
    "return all shortest paths" in {
      val spt = new ShortestPathsTraverser(TestGraphs.g8, Seq(1))
      spt.toList
      spt.numPaths(8) shouldEqual 3
      spt.paths(8) shouldEqual Seq(Seq(1, 2, 4, 6), Seq(1, 2, 5, 6), Seq(1, 3, 7, 6))

      val spt2 = new ShortestPathsTraverser(TestGraphs.g5, Seq(10))
      spt2.toList
      spt2.numPaths(14) shouldEqual 1
      spt2.paths(14) shouldEqual Seq(Seq(10,13))
    }

    "return different paths when direction is reversed" in {
      val spt = new ShortestPathsTraverser(TestGraphs.g8, Seq(1), GraphDir.InDir)
      spt.toList
      spt.numPaths(5) shouldEqual 2
      spt.paths(5) shouldEqual Seq(Seq(1, 8, 6), Seq(1, 9, 6))
    }

    "return no shortest paths longer than 3 when limit is set" in {
      val spt = new ShortestPathsTraverser(TestGraphs.g8, Seq(1), GraphDir.OutDir, new Walk.Limits(maxDepth = Some(3)))
      spt.toList
      spt.numPaths.getOrElse(8, 0) shouldEqual 0
      spt.paths.getOrElse(8, Seq(Seq())) shouldEqual Seq(Seq())
    }
  }


  "BoundedIterator" should {
    "satisfy a limit" in {
      val biter = new TestIter with BoundedIterator[Int] {
        lazy val maxSteps = Some(3L)
      }
      biter.toSeq.toList shouldEqual List(10, 20, 30)
    }

    "be unlimited when limit is None" in {
      val biter = new TestIter with BoundedIterator[Int] {
        lazy val maxSteps = None
      }
      biter.toSeq.toList shouldEqual List(10, 20, 30, 40)
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
      randomTraverser.next shouldEqual homeNode
      var curr = homeNode
      for (i <- 1 to 10) {
        val next = randomTraverser.next
        next should not be curr
        curr.isNeighbor(dir, next.id) shouldEqual true
        curr = next
      }
    }

    "test onlyOnce option on graph3, when it is set to true, walk should reset to homenode " +
        "when visit the same node twice, even when resetprob equals 0, " +
        "if option not set, walk will run into infinite loop" in {
      val mockRandom = mock[Random]
      //mock random always returns the last element
      when(mockRandom.nextDouble()).thenReturn(1.0)
      val graph3 = TestGraphs.g3
      def getNode(id: Int) = graph3.getNodeById(id).get

      val resetProb = 0.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(
            graph3, dir, Seq(10), resetProb, None, true, mockRandom, None, false)
        val homeNode = getNode(10)

        when(mockRandom.nextInt(1)).thenReturn(0)
        when(mockRandom.nextInt(2)).thenReturn(1)
        randomTraverser.next shouldEqual homeNode

        var next = randomTraverser.next
        next.id shouldEqual 12
        next = randomTraverser.next
        next.id shouldEqual 11
        //the set of edges are (10->11), (10->12), (11->12), (12->11)
        //if onceOnly is not set, we will visit 12
        //but because 12 has been seen, we reset to homenode
        next = randomTraverser.next
        next.id shouldEqual 10
      }
    }

    "test onlyOnce option on graph6, when it is set to true, walk should reset to homenode when " +
        "visit the same node twice, even when resetprob equals 0" in {
      val mockRandom = mock[Random]
      //mock random always returns the last element
      when(mockRandom.nextDouble()) thenReturn 1.0

      val resetProb = 0.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(graph, dir, Seq(12),
          resetProb, None, true, mockRandom, None, false)
        val homeNode = getNode(12)
        when(mockRandom.nextInt(1)) thenReturn 0
        randomTraverser.next shouldEqual homeNode
        var next = randomTraverser.next()
        next.id shouldEqual 14
        when(mockRandom.nextInt(2)) thenReturn 1
        next = randomTraverser.next()
        next.id shouldEqual 15
        //node 15 has 2 neighbors (10,11), we visit 11
        next = randomTraverser.next()
        next.id shouldEqual 11
        //11 has 2 neighbors (12,14), our mockRandom will pick 14
        //but 14 has been visited before, reset back to homenode(12)
        next = randomTraverser.next()
        next.id shouldEqual 12
      }
    }

    "always go to the home node when resetprob equals 1" in {
      val resetProb = 1.0
      List(GraphDir.OutDir, GraphDir.InDir) foreach { dir =>
        val dir = GraphDir.OutDir
        val randomTraverser = new RandomTraverser(graph, dir, Seq(10), resetProb,
          None, false, new Random, None, false)
        val homeNode = getNode(10)
        randomTraverser.next shouldEqual homeNode
        val curr = homeNode
        for (i <- 1 to 10) {
          randomTraverser.next shouldEqual curr
        }
      }
    }
  }

  "BreadthFirstTraverser" should {
    val graph = TestGraphs.g6

    "yield all nodes in BFS order in id walk with correct discovery times" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(10), Walk.Limits(Some(5),
          None, Some(10L)))
      val ids = bfs.toSeq.map { _.id }.toList

      ids shouldEqual List(10, 11, 12, 13, 14, 15)
      bfs.depth(10) shouldEqual Some(0)
      List(11, 12, 13) foreach { bfs.depth(_) shouldEqual Some(1) }
      List(14) foreach { bfs.depth(_) shouldEqual Some(2) }
      List(15) foreach { bfs.depth(_) shouldEqual Some(3) }
      bfs.depth(16) shouldEqual None
    }

    "yield all nodes in BFS order walk with constraint maxDepth" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(15), Walk.Limits(Some(1),
        None, Some(10L)), None)

      val ids = bfs.toSeq.map { _.id }.toList
      ids shouldEqual List(15, 10, 11)
      bfs.depth(15) shouldEqual Some(0)
      List(10, 11) foreach { bfs.depth(_) shouldEqual Some(1) }
      List(13, 16) foreach { bfs.depth(_) shouldEqual None }
    }

    "yield all nodes in BFS order walk with constraint numOfFriendsThresh" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(15), new Walk.Limits(5, 3, 10L))

      val ids = bfs.toSeq.map { _.id }.toList
      ids shouldEqual List(15, 10, 11, 12, 14)
      bfs.depth(15) shouldEqual Some(0)
      List(10, 11) foreach { bfs.depth(_) shouldEqual Some(1) }
      List(12, 14) foreach { bfs.depth(_) shouldEqual Some(2) }
      List(13, 16) foreach { bfs.depth(_) shouldEqual None }
    }

    "yield all nodes in BFS order walk with constraint maxSteps" in {
      val dir = GraphDir.OutDir
      val bfs = new BreadthFirstTraverser(graph, dir, Seq(15), new Walk.Limits(5, 3, 4L))

      val ids = bfs.toSeq.map { _.id }.toList
      ids shouldEqual List(15, 10, 11, 12)
      bfs.depth(15) shouldEqual Some(0)
      List(10, 11) foreach { bfs.depth(_) shouldEqual Some(1) }
      List(12) foreach { bfs.depth(_) shouldEqual Some(2) }
      List(13, 16) foreach { bfs.depth(_) shouldEqual None }
    }
  }

  "DepthFirstTraverser" should {
    "yield all nodes in DFS order with expected finishing times" in {
      val trav = new DepthFirstTraverser(TestGraphs.g5, GraphDir.OutDir, Seq(10))
        with DiscoveryAndFinishTimeTracker
      val ids = trav.toSeq.map(_.id).toList
      ids shouldEqual List(10, 11, 12, 13, 14)

      trav.finishingTime(10) shouldEqual Some(9)
      trav.finishingTime(11) shouldEqual Some(5)
      trav.finishingTime(12) shouldEqual Some(4)
      trav.finishingTime(13) shouldEqual Some(8)
      trav.finishingTime(14) shouldEqual Some(7)

      trav.discoveryTime(10) shouldEqual Some(0)
      trav.discoveryTime(11) shouldEqual Some(1)
      trav.discoveryTime(12) shouldEqual Some(2)
      trav.discoveryTime(13) shouldEqual Some(3)
      trav.discoveryTime(14) shouldEqual Some(6)
    }

    val graph = TestGraphs.g6
    "yield all nodes in DFS order in expected order (outDir)" in {
      val dir = GraphDir.OutDir
      val trav = new DepthFirstTraverser(graph, dir, Seq(10))
        with PathLengthTracker with DiscoveryAndFinishTimeTracker
      val ids = trav.toSeq.map { _.id }.toList

      ids shouldEqual List(10, 11, 12, 14, 15, 13)

      trav.distance(10) shouldEqual Some(0)
      trav.distance(11) shouldEqual Some(1)
      trav.distance(12) shouldEqual Some(2)
      trav.distance(14) shouldEqual Some(3)
      trav.distance(15) shouldEqual Some(4)
      trav.distance(13) shouldEqual Some(1)

      trav.finishingTime(10) shouldEqual Some(11)
      trav.finishingTime(11) shouldEqual Some(9)
      trav.finishingTime(12) shouldEqual Some(8)
      trav.finishingTime(13) shouldEqual Some(10)
      trav.finishingTime(14) shouldEqual Some(7)
      trav.finishingTime(15) shouldEqual Some(6)

      trav.discoveryTime(10) shouldEqual Some(0)
      trav.discoveryTime(11) shouldEqual Some(1)
      trav.discoveryTime(12) shouldEqual Some(2)
      trav.discoveryTime(13) shouldEqual Some(3)
      trav.discoveryTime(14) shouldEqual Some(4)
      trav.discoveryTime(15) shouldEqual Some(5)
    }

    "yield all nodes in DFS order in expected order in unique id walk (inDir)" in {
      val dir = GraphDir.InDir
      val trav = new DepthFirstTraverser(graph, dir, Seq(10), Walk.Limits(None, None, Some(10L)))
        with PathLengthTracker with DiscoveryAndFinishTimeTracker
      val ids = trav.toSeq.map { _.id }.toList

      ids shouldEqual List(10, 15, 14, 11, 12, 13)

      trav.distance(10) shouldEqual Some(0)
      trav.distance(15) shouldEqual Some(1)
      trav.distance(14) shouldEqual Some(2)
      trav.distance(11) shouldEqual Some(3)
      trav.distance(12) shouldEqual Some(3)
      trav.distance(13) shouldEqual Some(4)

      trav.finishingTime(10) shouldEqual Some(11)
      trav.finishingTime(11) shouldEqual Some(6)
      trav.finishingTime(12) shouldEqual Some(8)
      trav.finishingTime(13) shouldEqual Some(7)
      trav.finishingTime(14) shouldEqual Some(9)
      trav.finishingTime(15) shouldEqual Some(10)

      trav.discoveryTime(10) shouldEqual Some(0)
      trav.discoveryTime(11) shouldEqual Some(3)
      trav.discoveryTime(12) shouldEqual Some(4)
      trav.discoveryTime(13) shouldEqual Some(5)
      trav.discoveryTime(14) shouldEqual Some(2)
      trav.discoveryTime(15) shouldEqual Some(1)
    }

    "yield all nodes of Bipartite Graph Single Side in expected DFS order" in {
      val dir = GraphDir.OutDir
      val graph = TestGraphs.bipartiteGraphSingleSide

      var dfs = new DepthFirstTraverser(graph, dir, Seq(-2, -5))
      var ids = dfs.toSeq.map { _.id }.toList
      ids shouldEqual List(-2, 5, 10, -5, 8)

      dfs = new DepthFirstTraverser(graph, dir, Seq(5))
      ids = dfs.toSeq.map { _.id }.toList
      ids shouldEqual List(5)
    }

    "yield all nodes of Bipartite Graph Double Side in expected DFS order" in {
      val dir = GraphDir.InDir
      val graph = TestGraphs.bipartiteGraphDoubleSide
      val dfs = new DepthFirstTraverser(graph, dir, Seq(-1))
      val ids = dfs.toSeq.map { _.id }.toList
      ids shouldEqual List(-1, 4, 5, -2, -5, 10, 123)
    }
  }

  "AllPathsWalk" should {
    val graph = TestGraphs.g6

    "yield nodes in All Paths Walk with first visit depth (outDir)" in {
      val dir = GraphDir.OutDir
      val trav = new AllPathsWalk(graph, dir, Seq(10), Walk.Limits(Some(2), None, Some(6)))
      val ids = trav.toSeq.map { _.id }.toList
      ids shouldEqual List(10, 11, 12, 13, 12, 14)

      trav.depth(10) shouldEqual Some(0)
      trav.depth(11) shouldEqual Some(1)
      trav.depth(12) shouldEqual Some(1)
      trav.depth(13) shouldEqual Some(1)
      trav.depth(14) shouldEqual Some(2)
    }

    "yield all nodes in All Paths Walk with first visit depth (inDir)" in {
      val dir = GraphDir.InDir
      val trav = new AllPathsWalk(graph, dir, Seq(10), Walk.Limits(None, None, Some(10)))
      val ids = trav.toSeq.map { _.id }.toList
      ids shouldEqual List(10, 15, 14, 11, 12, 13, 10, 15, 10, 11)

      trav.depth(10) shouldEqual Some(0)
      trav.depth(15) shouldEqual Some(1)
      trav.depth(14) shouldEqual Some(2)
      trav.depth(11) shouldEqual Some(3)
      trav.depth(12) shouldEqual Some(3)
      trav.depth(13) shouldEqual Some(3)
    }
  }
}
