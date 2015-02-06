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

import org.scalatest.WordSpec

class ArrayBasedDirectedGraphSpec extends WordSpec with GraphBehaviours {
  verifyGraphBuilding(ArrayBasedDirectedGraph.apply(_, _, NeighborsSortingStrategy.LeaveUnsorted),
    sampleGraphEdges)

  "ArrayBasedDirectedGraph" when {
    "reading neighbors without sorting" should {
      val graph = ArrayBasedDirectedGraph.apply(
        Iterable(
          NodeIdEdgesMaxId(1, Array(2, 3)),
          NodeIdEdgesMaxId(2, Array(3, 1)),
          NodeIdEdgesMaxId(3, Array(1, 2))), StoredGraphDir.BothInOut,
        NeighborsSortingStrategy.LeaveUnsorted)

      verifyInOutEdges(graph, 3,
        Map(1 -> Seq(2, 3), 2 -> Seq(3, 1), 3 -> Seq(1, 2)),
        Map(1 -> Seq(2, 3), 2 -> Seq(1, 3), 3 -> Seq(1, 2)),
        checkOrdering = true)
    }

    "reading sorted neighbors and returning graph with sorted neighbors" should {
      val graph = ArrayBasedDirectedGraph.apply(
        Iterable(
          NodeIdEdgesMaxId(3, Array(1, 2)),
          NodeIdEdgesMaxId(1, Array(2, 3)),
          NodeIdEdgesMaxId(2, Array(1, 3))
        ), StoredGraphDir.BothInOut,
        NeighborsSortingStrategy.AlreadySorted)

      verifyInOutEdges(graph, 3,
        Map(1 -> Seq(2, 3), 2 -> Seq(1, 3), 3 -> Seq(1, 2)),
        Map(1 -> Seq(2, 3), 2 -> Seq(1, 3), 3 -> Seq(1, 2)),
        checkOrdering = true)
    }

    "sorting neighbors while reading" should {
      val graph = ArrayBasedDirectedGraph.apply(
        Iterable(
          NodeIdEdgesMaxId(3, Array(2, 1)),
          NodeIdEdgesMaxId(1, Array(3, 2)),
          NodeIdEdgesMaxId(2, Array(3, 1))
        ), StoredGraphDir.BothInOut,
        NeighborsSortingStrategy.SortWhileReading)

      verifyInOutEdges(graph, 3,
        Map(1 -> Seq(2, 3), 2 -> Seq(1, 3), 3 -> Seq(1, 2)),
        Map(1 -> Seq(2, 3), 2 -> Seq(1, 3), 3 -> Seq(1, 2)),
        checkOrdering = true)
    }
  }
}
