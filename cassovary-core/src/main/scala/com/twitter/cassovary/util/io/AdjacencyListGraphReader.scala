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

import com.twitter.cassovary.graph.{NodeIdEdgesMaxId, StoredGraphDir}
import com.twitter.cassovary.graph.StoredGraphDir._
import com.twitter.cassovary.util.{NodeNumberer, ParseString}
import com.twitter.util.NonFatal
import java.io.{BufferedInputStream, FileInputStream, IOException}
import java.util.zip.GZIPInputStream
import scala.io.Source

/**
 * Reads in a multi-line adjacency list from multiple files in a directory, where ids are of type T.
 * Does not check for duplicate edges or nodes.
 *
 * You can optionally specify which files in a directory to read. For example, you may have files starting with
 * "part-" that you'd like to read. Only these will be read in if you specify that as the file prefix.
 *
 * In each file, a node and its neighbors is defined by the first line being that
 * node's id and its # of neighbors, followed by that number of ids on subsequent lines.
 * For example, when ids are Ints,
 *    241 3
 *    2
 *    4
 *    1
 *    53 1
 *    241
 *    ...
 * In this file, node 241 has 3 neighbors, namely 2, 4 and 1. Node 53 has 1 neighbor, 241.
 *
 * Similarly, when ids are String, input file should follow the example:
 *    Alice 2
 *    Bob
 *    Chris
 *    Bob 1
 *    Chris
 *    Chris 1
 *    Bob
 *    ...
 * In this file Alice has 2 directed edges to Bob and Chris, Bob has an edge to Chris,
 * and Chris has outgoing edge to Bob.
 * *
 * @param directory the directory to read from
 * @param prefixFileNames the string that each part file starts with
 * @param nodeNumberer nodeNumberer to use with node ids
 * @param idReader function that can read id from String
 */
class AdjacencyListGraphReader[T] (
  val directory: String,
  override val prefixFileNames: String = "",
  val nodeNumberer: NodeNumberer[T],
  idReader: (String => T),
  isGzip: Boolean = false,
  separator: Char = ' '
) extends GraphReaderFromDirectory[T] {


  /**
   * Read in nodes and edges from a single file
   * @param filename Name of file to read from
   */
  private class OneShardReader(filename: String, nodeNumberer: NodeNumberer[T])
    extends Iterable[NodeIdEdgesMaxId] {

    def adjacencyNodeIdsIterator(): Iterator[(Int, Option[Int])] = {
      new AdjacencyTsFileReader[T](filename, separator, idReader) map { case (nodeId, count) =>
        val internalNodeId = nodeNumberer.externalToInternal(nodeId)
        (internalNodeId, count)
      }
    }

    override def iterator = new Iterator[NodeIdEdgesMaxId] {

      private val nodeIds = adjacencyNodeIdsIterator()

      override def hasNext: Boolean = nodeIds.hasNext

      override def next(): NodeIdEdgesMaxId = {
        var i = 0
        val (id, outEdgeCountIntOpt) = nodeIds.next()
        val internalNodeId = id

        var newMaxId = internalNodeId
        val outEdgeCountInt = outEdgeCountIntOpt.get
        val outEdgesArr = new Array[Int](outEdgeCountInt)
        while (i < outEdgeCountInt) {
          val (internalNghId, tmp) = nodeIds.next()
          newMaxId = newMaxId max internalNghId
          outEdgesArr(i) = internalNghId
          i += 1
        }

        NodeIdEdgesMaxId(internalNodeId, outEdgesArr, newMaxId)
      }
    }
  }

  def oneShardReader(filename : String) : Iterable[NodeIdEdgesMaxId] = {
    new OneShardReader(filename, nodeNumberer)
  }

  // note that we are assuming that n.id.toString does the right thing, which is
  // true for int and long ids but might not be for a general T.
  def reverseParseNode(n: NodeIdEdgesMaxId): String = {
    n.id + separator.toString + n.edges.length + "\n" + n.edges.mkString("\n") + "\n"
  }

}

object AdjacencyListGraphReader {
  def forIntIds(directory: String, prefixFileNames: String = "",
      nodeNumberer: NodeNumberer[Int] = new NodeNumberer.IntIdentity(),
      graphDir: StoredGraphDir = StoredGraphDir.OnlyOut, isGzip: Boolean = false, separator: Char = ' ') =
    new AdjacencyListGraphReader[Int](directory, prefixFileNames, nodeNumberer, ParseString.toInt, isGzip = isGzip) {
      override def storedGraphDir: StoredGraphDir = graphDir
    }
}
