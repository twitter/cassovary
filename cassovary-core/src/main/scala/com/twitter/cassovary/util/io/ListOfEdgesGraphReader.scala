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

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.util.collections.ReusableArrayBuffer
import com.twitter.cassovary.util.{NodeNumberer, ParseString}
import com.twitter.cassovary.graph.{StoredGraphDir, NodeIdEdgesMaxId}
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap


/**
 * Reads in a multi-line list of edges from multiple files in a directory.
 * Each edge is in its own line and is of the form:
 * source-id<separator>destination-id
 * where separator is a single character.
 *
 * One can optionally specify which files in a directory to read.
 * For example, one may have files starting with
 * "part-" that one would like to read, perhaps containing subgraphs of one single graph.
 *
 * One can optionally specify two additional operations during reading:
 * - to remove duplicate edges
 * - to sort list of adjacent nodes
 *
 * In each file, a directed edges is defined by a pair of T: from and to.
 * For example, we use `String` ids with ` ` (space) `separator`, when
 * reading file:
 * {{{
 * a b
 * b d
 * d c
 * a e
 * ...
 * }}}
 * In this file, node `a` has two outgoing edges (to `b` and `e`), node `b` has an outgoing edge
 * to node `d` and node `d` has an outgoing edge to node `c`.
 *
 * Note that, it is recommended to use AdjacencyListGraphReader, because of its efficiency.
 *
 * @param directory the directory to read from
 * @param prefixFileNames the string that each part file starts with
 * @param nodeNumberer nodeNumberer to use with node ids
 * @param idReader function that can read id from String
 * @param removeDuplicates if false (default), the edges are guaranteed to be unique
 * @param sortNeighbors if true, the neighbors of a node should be sorted by this class
 * @param separator the character that separates the source and destination ids
 * @param sortedBySourceId if true (default), all the edges belonging to the same sourceId are given
 *                         on consecutive lines. This allows efficient internal data structure.
 */

class ListOfEdgesGraphReader[T](
    val directory: String,
    override val prefixFileNames: String,
    val nodeNumberer: NodeNumberer[T],
    idReader: (String, Int, Int) => T,
    removeDuplicates: Boolean = false,
    sortNeighbors: Boolean = false,
    separator: Char = ' ',
    sortedBySourceId: Boolean = true,
    isGzip: Boolean = false
    ) extends GraphReaderFromDirectory[T] {

  private class OneShardReader(filename: String, nodeNumberer: NodeNumberer[T])
    extends Iterable[NodeIdEdgesMaxId] {

    def twoNodeIdsIterator(): Iterator[(Int, Int)] = {
      new TwoTsFileReader[T](filename, separator, idReader, isGzip) map { case (source, dest) =>
        val internalFromId = nodeNumberer.externalToInternal(source)
        val internalToId = nodeNumberer.externalToInternal(dest)
        (internalFromId, internalToId)
      }
    }

    // accumulate neighbors if given in sorted order of fromId
    private def edgesSourceSorted() = new Iterable[ReusableArrayBuffer] {

      def iterator = new Iterator[ReusableArrayBuffer] {

        private val bufs = Array.fill[ReusableArrayBuffer](2)(new ReusableArrayBuffer())
        private var nextIndx = 0
        private def startNewBuf(source: Int, dest: Int) = {
          nextIndx = 1 - nextIndx
          val buf = bufs(nextIndx)
          buf.clear()
          buf += source //head of buf is always source
          buf += dest
          buf
        }

        private val nodeIds = twoNodeIdsIterator()

        private var _next = {
          if (nodeIds.hasNext) {
            val (from, to) = nodeIds.next()
            Some(startNewBuf(from, to))
          }
          else None
        }

        def hasNext: Boolean = _next.isDefined

        def next() = {
          val curr = _next.get
          val expectedFromId = curr(0)
          _next = nodeIds find { case (from, to) =>
            val accum = from == expectedFromId
            if (accum) curr += to
            !accum
          } map { case (from, to) =>
            startNewBuf(from, to)
          }
          curr
        }

      }
    }

    // accumulate neighbors if given in sorted order of fromId
    private def edgesSourceMap() = new Iterable[ReusableArrayBuffer] {

      def readEdgesBySource(): Int2ObjectOpenHashMap[ReusableArrayBuffer] = {
        val edgesBySource = new Int2ObjectOpenHashMap[ReusableArrayBuffer]()
        val nodeIds = twoNodeIdsIterator()
        nodeIds foreach { case (internalFromId, internalToId) =>
          if (edgesBySource.containsKey(internalFromId)) {
              edgesBySource.get(internalFromId) += internalToId
          } else {
            val buf = new ReusableArrayBuffer(4)
            buf += internalFromId
            buf += internalToId
            edgesBySource.put(internalFromId, buf)
          }
        }
        edgesBySource
      }

      private lazy val _edgesSource = readEdgesBySource().int2ObjectEntrySet()

      def iterator = new Iterator[ReusableArrayBuffer] {
        private val _edgesIterator = _edgesSource.fastIterator()

        def hasNext: Boolean = _edgesIterator.hasNext

        def next(): ReusableArrayBuffer = {
          val entry = _edgesIterator.next()
          entry.getValue
        }
      }
    }

    private lazy val idAndEdgesIterable = {
      if (sortedBySourceId) edgesSourceSorted()
      else edgesSourceMap()
    }

    def iterator = new Iterator[NodeIdEdgesMaxId] {

      private lazy val idAndEdgesIterator = idAndEdgesIterable.iterator

      def hasNext: Boolean = idAndEdgesIterator.hasNext

      def next() = {

        def prepareEdges(buf: Array[Int]) : (Array[Int], Int) = {
          (removeDuplicates, sortNeighbors) match {
            case (false, false) => (buf, buf.max)
            case (true, false) =>
              val b = buf.distinct
              (b, b.max)
            case (false, true) =>
              val b = buf.sorted
              (b, b(b.length - 1))
            case (true, true) =>
              val b = buf.sorted.distinct
              (b, b(b.length - 1))
          }
        }

        val buf = idAndEdgesIterator.next()
        val id = buf(0)
        val edgesArr = buf.toArray(1)
        val (edges, edgesMaxId) = prepareEdges(edgesArr)
        val nodeMaxId = id.max(edgesMaxId)
        NodeIdEdgesMaxId(id, edges, nodeMaxId)
      }
    }
  }

  def oneShardReader(filename: String): Iterable[NodeIdEdgesMaxId] = {
    new OneShardReader(filename, nodeNumberer)
  }

  def reverseParseNode(n: NodeIdEdgesMaxId): String = {
    n.edges.map { neighbor =>
      n.id + " " + neighbor
    }.mkString("\n") + "\n"
  }

}

object ListOfEdgesGraphReader {
  def forIntIds(directory: String, prefixFileNames: String = "",
      nodeNumberer: NodeNumberer[Int] = new NodeNumberer.IntIdentity(),
      removeDuplicates: Boolean = false,
      sortNeighbors: Boolean = false,
      separator: Char = ' ',
      graphDir: StoredGraphDir = StoredGraphDir.OnlyOut,
      isGzip: Boolean = false) =
    new ListOfEdgesGraphReader[Int](directory, prefixFileNames,
      new NodeNumberer.IntIdentity(), ParseString.toInt, removeDuplicates,
      sortNeighbors, separator, isGzip = isGzip) {
      override def storedGraphDir: StoredGraphDir = graphDir
    }
}
