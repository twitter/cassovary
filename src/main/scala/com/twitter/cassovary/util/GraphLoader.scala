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

import com.twitter.cassovary.graph.{StoredGraphDir, NodeIdEdgesMaxId, CachedDirectedGraph}
import io.Source
import java.io.File
import java.util.concurrent.{Executors, ExecutorService}
import com.twitter.cassovary.graph.StoredGraphDir._

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
  def apply(directory: String, cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Long,
            shardDirectory: String, numShards: Int, numRounds: Int,
            useCachedValues: Boolean, cacheDirectory: String) = {
    new GraphLoader().loadGraphFromDirectory(directory, cacheType, cacheMaxNodes, cacheMaxEdges,
      shardDirectory, numShards, numRounds,
      useCachedValues, cacheDirectory)
  }
}

private class GraphLoader {

  class RawEdgeShardReader(filename: String) extends Iterator[NodeIdEdgesMaxId] {

    private val outEdgePattern = """^(?:src= )?(\d+)[\cA\t ](?:#= )?(\d+)""".r
    val lines = Source.fromFile(filename).getLines()
    private val holder = NodeIdEdgesMaxId(-1, null, -1)

    override def hasNext: Boolean = lines.hasNext

    override def next(): NodeIdEdgesMaxId = {
      val outEdgePattern(id, outEdgeCount) = lines.next.trim
      var i = 0
      var currIndex = 0
      val outEdgeCountInt = outEdgeCount.toInt
      val idInt = id.toInt

      var newMaxId = idInt
      val outEdgesArr = new Array[Int](outEdgeCountInt)
      while (i < outEdgeCountInt) {
        val edgeId = lines.next.trim.toInt
        newMaxId = newMaxId max edgeId
        outEdgesArr(currIndex) = edgeId
        currIndex += 1
        i += 1
      }

      holder.id = idInt
      holder.edges = outEdgesArr
      holder.maxId = newMaxId
      holder
    }

  }

  class RawEdgeShardsReader(directory: String) {
    val dir = new File(directory)

    def readers: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
      val validFiles = dir.list().map({ filename =>
        if (filename.startsWith("part")) {
          filename
        }
        else {
          null
        }
      }).filterNot(f => f == null)
      validFiles.map({ filename =>
        {() => new RawEdgeShardReader(directory + "/" + filename)}
      }).toSeq
    }
  }

  // Return a CachedDirectedGraph object
  def loadGraphFromDirectory(directory: String, cacheType: String, cacheMaxNodes:Int, cacheMaxEdges:Long,
                             shardDirectory: String, numShards: Int, numRounds: Int,
                             useCachedValues: Boolean, cacheDirectory: String) = {
    // TODO Support OnlyIn as well
    CachedDirectedGraph(new RawEdgeShardsReader(directory).readers, Executors.newFixedThreadPool(8),
      StoredGraphDir.OnlyOut, cacheType, cacheMaxNodes, cacheMaxEdges,
      shardDirectory, numShards, numRounds, useCachedValues, cacheDirectory, true)
  }

}
