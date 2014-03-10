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

import com.twitter.cassovary.graph._
import com.google.common.util.concurrent.MoreExecutors
import com.twitter.cassovary.graph.StoredGraphDir
import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import java.util.concurrent.ExecutorService
import java.io.File
import com.twitter.cassovary.util.NodeRenumberer

/**
 * Trait that classes should implement to read in graphs.
 *
 * The reader class is only required to implement iteratorSeq, a method
 * which returns a sequence of functions that themselves return an Iterator
 * over NodeIdEdgesMaxId (see its type signature below as well).
 *
 * NodeIdEdgesMaxId is a case class defined in ArrayBasedDirectedGraph
 * that stores 1) the id of a node, 2) the ids of its neighbors,
 * and 3) the maximum id of itself and its neighbors.
 *
 * One useful reference implementation is AdjacencyListGraphReader.
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
   *                  128 is an arbitrary default
   */
  def toSharedArrayBasedDirectedGraph(numShards: Int = 128) = {
    SharedArrayBasedDirectedGraph(iteratorSeq, executorService, storedGraphDir, numShards)
  }

  trait ShardReader extends Iterator[NodeIdEdgesMaxId]

  trait ShardReaderCompanion {
    def apply(filename : String, nodeRenumberer : NodeRenumberer) : ShardReader
  }

  val shardReaderCompanion : ShardReaderCompanion

  /**
   * Read in nodes and edges from multiple files. Use shardReaderCompanion
   * for reading a single file.
   * @param directory Directory to read from
   * @param prefixFileNames the string that each part file starts with
   */
  class ShardsReader(directory: String, prefixFileNames: String = "",
                     nodeRenumberer : NodeRenumberer) {
    val dir = new File(directory)

    def readers: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
      val validFiles = dir.list().flatMap({ filename =>
        if (filename.startsWith(prefixFileNames)) {
          Some(filename)
        }
        else {
          None
        }
      })
      validFiles.map({ filename =>
      {() => shardReaderCompanion(directory + "/" + filename, nodeRenumberer)}
      }).toSeq
    }
  }
}
