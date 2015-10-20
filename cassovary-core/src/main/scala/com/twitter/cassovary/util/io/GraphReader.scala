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

import com.twitter.cassovary.graph.NeighborsSortingStrategy.NeighborsSortingStrategy
import com.twitter.cassovary.graph.NeighborsSortingStrategy.LeaveUnsorted
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.graph._
import com.twitter.cassovary.util.NodeNumberer

/**
 * Trait that classes should implement to read in graphs that nodes have
 * ids of type `T`.
 *
 * The reader class is required to implement `iteratorSeq`, a method
 * which returns a sequence of functions that themselves return an `Iterator`
 * over `NodeIdEdgesMaxId` (see its type signature below as well).
 *
 * It is also required to provide a `nodeNumberer[T]`.
 *
 * `NodeIdEdgesMaxId` is a case class defined in `ArrayBasedDirectedGraph`
 * that stores 1) the id of a node, 2) the ids of its neighbors,
 * and 3) the maximum id of itself and its neighbors.
 *
 * One useful reference implementation is `AdjacencyListGraphReader`.
 */
trait GraphReader[T] {
  /**
   * Should return a sequence of `NodeIdEdgesMaxId` iterables
   */
  def iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]]

  /**
   * Define node numberer
   */
  def nodeNumberer : NodeNumberer[T]

  /**
   * Override to modify the graph's stored direction
   */
  def storedGraphDir: StoredGraphDir = StoredGraphDir.OnlyOut

  def parallelismLimit: Int = Runtime.getRuntime.availableProcessors

  /**
   * The reader knows the format as it knows how to read the file. This reverse parses
   * the input `n` to a string in that same format.
   */
  def reverseParseNode(n: NodeIdEdgesMaxId): String

  /**
   * Create an `ArrayBasedDirectedGraph`
   */
  def toArrayBasedDirectedGraph(neighborsSortingStrategy: NeighborsSortingStrategy = LeaveUnsorted,
      forceSparseRepr: Option[Boolean] = None) = {
    ArrayBasedDirectedGraph(iterableSeq, parallelismLimit, storedGraphDir, neighborsSortingStrategy,
      forceSparseRepr)
  }

  def toSharedArrayBasedDirectedGraph(forceSparseRepr: Option[Boolean] = None) = {
    SharedArrayBasedDirectedGraph(iterableSeq, parallelismLimit, storedGraphDir, forceSparseRepr)
  }

  /**
   * Create an `ArrayBasedDynamicDirectedGraph`
   */
  def toArrayBasedDynamicDirectedGraph() = {
    new ArrayBasedDynamicDirectedGraph(iterableSeq, storedGraphDir)
  }
}
