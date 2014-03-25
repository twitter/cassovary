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

import com.twitter.cassovary.util.NodeRenumberer
import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import it.unimi.dsi.fastutil.ints.{Int2ObjectMap, Int2ObjectLinkedOpenHashMap}
import scala.io.Source
import scala.collection.mutable.ListBuffer

/**
 * Reads in a multi-line list of edges from multiple files in a directory.
 * Does not check for duplicate edges or nodes.
 *
 * You can optionally specify which files in a directory to read. For example, you may have files starting with
 * "part-" that you'd like to read. Only these will be read in if you specify that as the file prefix.
 *
 * In each file, a directed edges is defined by a pair of integers: from and to.
 * For example,
 * 1 3
 * 2 4
 * 4 3
 * 1 5
 * ...
 * In this file, node 1 has two outgoing edges (to 3 and 5), node 2 has an outgoing edge
 * to node 4 and node 4 has an outgoing edge to node 3.
 *
 * Note that, it is recommended to use AdjacencyListGraphReader, because of its efficiency.
 *
 * @param directory the directory to read from
 * @param prefixFileNames the string that each part file starts with
 */
class ListOfEdgesGraphReader(val directory: String, val prefixFileNames: String,
                             val nodeRenumberer: NodeRenumberer = new NodeRenumberer.Identity())
    extends GraphReaderFromDirectory {

  class OneShardReader(filename: String, nodeRenumberer: NodeRenumberer)
    extends Iterator[NodeIdEdgesMaxId] {

    private val holder = NodeIdEdgesMaxId(-1, null, -1)

    def readEdgesBySource(): Int2ObjectMap[ListBuffer[Int]] = {
      val directedEdgePattern = """^(\d+)\s+(\d+)""".r
      val commentPattern = """(^#.*)""".r
      val lines = Source.fromFile(filename).getLines()

      val edgesBySource = new Int2ObjectLinkedOpenHashMap[ListBuffer[Int]]()

      lines.foreach {
        line =>
          line.trim match {
            case commentPattern(s) => ()
            case directedEdgePattern(from, to) =>
              val internalFromId = nodeRenumberer.externalToInternal(from.toInt)
              if (edgesBySource.containsKey(internalFromId)) {
                edgesBySource.get(internalFromId) += nodeRenumberer.externalToInternal(to.toInt)
              } else {
                edgesBySource.put(internalFromId, ListBuffer(nodeRenumberer.externalToInternal(to.toInt)))
              }
          }
      }
      edgesBySource
    }

    lazy val edgesIterator = readEdgesBySource().entrySet().iterator()

    override def hasNext: Boolean = edgesIterator.hasNext

    override def next(): NodeIdEdgesMaxId = {
      val elem = edgesIterator.next()
      holder.id = elem.getKey
      holder.edges = elem.getValue.toArray
      holder.maxId = holder.id max holder.edges.max
      holder
    }
  }

  override def oneShardReader(filename : String) : Iterator[NodeIdEdgesMaxId] = {
    new OneShardReader(filename, nodeRenumberer)
  }

  /**
   * Should return a sequence of iterators over NodeIdEdgesMaxId objects
   */
  override def iteratorSeq: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
    new ShardsReader(directory, prefixFileNames, nodeRenumberer).readers
  }
}
