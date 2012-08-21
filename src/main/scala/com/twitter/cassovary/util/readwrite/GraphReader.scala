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
package com.twitter.cassovary.util.readwrite

import com.twitter.cassovary.graph._
import java.util.concurrent.ExecutorService
import com.google.common.util.concurrent.MoreExecutors
import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir

/**
 * Trait that classes should implement to read in graphs
 * A reader class only needs to implement iteratorSeq, which should
 * return a sequence of () => Iterator[NodeIdEdgesMaxId]
 *
 * Optionally, a class or instance can override storedGraphDir which
 * defines the direction being read in, as well as executorService
 * (which can change how many threads to use).
 */
trait GraphReader {

  /**
   * Should return a sequence of iterators over NodeIdEdgesMaxId objects
   */
  def iteratorSeq: Seq[() => Iterator[NodeIdEdgesMaxId]]

  /**
   * Override to modify the graph's stored direction
   */
  def storedGraphDir: StoredGraphDir = StoredGraphDir.OnlyOut

  /**
   * Override to use multiple threads
   */
  def executorService: ExecutorService = MoreExecutors.sameThreadExecutor()

  /**
   * Create an ArrayBasedDirectedGraph
   */
  def toArrayBasedDirectedGraph() = {
    ArrayBasedDirectedGraph(iteratorSeq, executorService, storedGraphDir)
  }

  /**
   * Create a SharedArrayBasedDirectedGraph
   * @param numShards Number of shards to split the in-memory array into
   */
  def toSharedArrayBasedDirectedGraph(numShards: Int = 128) = {
    SharedArrayBasedDirectedGraph(iteratorSeq, executorService, storedGraphDir, numShards)
  }

}