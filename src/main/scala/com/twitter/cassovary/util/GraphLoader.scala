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
package com.twitter.cassovary.util

import com.twitter.cassovary.graph.{StoredGraphDir, CachedDirectedGraph}
import java.util.concurrent.Executors
import io.AdjacencyListGraphReader

/**
 * Loads a graph from a directory
 * The directory should contain part-r-XXXXX files containing edge data
 * These part files should be in the following format:
 *    241 3
 *    2
 *    4
 *    1
 *    53 1
 *    241
 *    ...
 * In other words, a node and its neighbors is defined by the first line being that
 * node's id and its # of neighbors, followed by that number of ids on subsequent lines.
 * In the above example, node 241 has 3 neighbors, namely 2, 4 and 1. Node 53 has 1 neighbor, 241.
 */
object GraphLoader {
  def apply(directory: String, directoryIn: Option[String], cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Long,
            shardDirectories: Array[String], inShardDirectories: Array[String], numShards: Int, numRounds: Int,
            useCachedValues: Boolean, cacheDirectory: String) = {
    new GraphLoader().loadGraphFromDirectory(directory, directoryIn, cacheType, cacheMaxNodes, cacheMaxEdges,
      shardDirectories, inShardDirectories, numShards, numRounds,
      useCachedValues, cacheDirectory)
  }
}

class GraphLoader {

  // Return a CachedDirectedGraph object
  def loadGraphFromDirectory(directory: String, directoryIn: Option[String], cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Long,
                             shardDirectories: Array[String], inShardDirectories: Array[String], numShards: Int, numRounds: Int,
                             useCachedValues: Boolean, cacheDirectory: String) = {
    directoryIn match {
      case Some(directory2) => CachedDirectedGraph(new AdjacencyListGraphReader(directory, "part").iteratorSeq,
        new AdjacencyListGraphReader(directory2, "part").iteratorSeq,
        Executors.newFixedThreadPool(8),
        StoredGraphDir.OnlyOut, cacheType, cacheMaxNodes, cacheMaxEdges,
        shardDirectories, inShardDirectories, numShards, numRounds, useCachedValues, cacheDirectory, true)
      case None => CachedDirectedGraph(new AdjacencyListGraphReader(directory, "part").iteratorSeq, null,
        Executors.newFixedThreadPool(8),
        StoredGraphDir.OnlyOut, cacheType, cacheMaxNodes, cacheMaxEdges,
        shardDirectories, inShardDirectories, numShards, numRounds, useCachedValues, cacheDirectory, true)
    }
  }
}
