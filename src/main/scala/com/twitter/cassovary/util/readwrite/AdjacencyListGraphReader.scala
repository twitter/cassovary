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

import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import io.Source
import java.io.File

/**
 * Reads in a multi-line adjacency list from multiple files in a directory.
 * You can optionally specify which files in a directory to read. For example, you may have files named
 * part-0.txt to part-10.txt and these will all be read in if you specify "part-" as the starting string.
 *
 * In each file, a node and its neighbors is defined by the first line being that
 * node's id and its # of neighbors, followed by that number of ids on subsequent lines.
 * For example,
 *    241 3
 *    2
 *    4
 *    1
 *    53 1
 *    241
 *    ...
 * In this file, node 241 has 3 neighbors, namely 2, 4 and 1. Node 53 has 1 neighbor, 241.
 *
 * @param directory the directory to read from
 * @param partFilesStartWith the string that each part file starts with
 */
class AdjacencyListGraphReader (directory: String, partFilesStartWith: String = "") extends GraphReader {

  /**
   * Read in nodes and edges from a single file
   * @param filename Name of file to read from
   */
  class ShardReader(filename: String) extends Iterator[NodeIdEdgesMaxId] {

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

  /**
   * Read in nodes and edges from multiple files
   * @param directory Directory to read from
   * @param partFilesStartWith the string that each part file starts with
   */
  class ShardsReader(directory: String, partFilesStartWith: String = "") {
    val dir = new File(directory)

    def readers: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
      val validFiles = dir.list().map({ filename =>
        if (filename.startsWith(partFilesStartWith)) {
          filename
        }
        else {
          null
        }
      }).filterNot(f => f == null)
      validFiles.map({ filename =>
      {() => new ShardReader(directory + "/" + filename)}
      }).toSeq
    }
  }

  def iteratorSeq = {
    new ShardsReader(directory, partFilesStartWith).readers
  }

}
