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

import java.io.IOException

import com.twitter.cassovary.graph.StoredGraphDir.StoredGraphDir
import com.twitter.cassovary.graph.{NodeIdEdgesMaxId, StoredGraphDir}
import com.twitter.cassovary.util.collections.FastMap
import com.twitter.cassovary.util.{NodeNumberer, ParseString}
import com.twitter.logging.Logger
import com.twitter.util.NonFatal

import scala.collection.mutable.ArrayBuffer
import scala.io.Source

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
 * For a default version for `Int` graphs see [[ListOfEdgesGraphReader.forIntIds]] builder method.
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
 */
class ListOfEdgesGraphReader[T](
    val directory: String,
    override val prefixFileNames: String,
    val nodeNumberer: NodeNumberer[T],
    idReader: (String, Int, Int) => T,
    removeDuplicates: Boolean = false,
    sortNeighbors: Boolean = false,
    separator: Char = ' '
    ) extends GraphReaderFromDirectory[T] {

  private lazy val log = Logger.get

  private class OneShardReader(filename: String, nodeNumberer: NodeNumberer[T])
    extends Iterable[NodeIdEdgesMaxId] {

    override def iterator = new Iterator[NodeIdEdgesMaxId] {

      var lastLineParsed = 0

      def readEdgesBySource(): FastMap[Int, ArrayBuffer[Int]] = {
        log.info("Starting reading from file %s...\n", filename)
        //val directedEdgePattern = ("""^(\w+)""" + separator + """(\w+)""").r
        val lines = Source.fromFile(filename).getLines()
          .map{x => {lastLineParsed += 1; x}}

        val edgesBySource = FastMap[Int, ArrayBuffer[Int]]()

        lines.foreach { line1 =>
          val line = line1.trim
          if (line.charAt(0) != '#') {
            val i = line.indexOf(separator)
            val source = idReader(line, 0, i - 1)
            val dest = idReader(line, i + 1, line.length - 1)
            val internalFromId = nodeNumberer.externalToInternal(source)
            val internalToId = nodeNumberer.externalToInternal(dest)
            if (edgesBySource.contains(internalFromId)) {
              edgesBySource.get(internalFromId) += internalToId
            } else {
              edgesBySource += (internalFromId, ArrayBuffer(internalToId))
            }
          }
        }
        log.info("Finished reading from file %s...\n", filename)
        Source.fromFile(filename).close()
        edgesBySource
      }

      val edgesBySource = readEdgesBySource()

      private lazy val edgesIterator = edgesBySource.asScala().iterator

      override def hasNext: Boolean = edgesIterator.hasNext

      override def next(): NodeIdEdgesMaxId = {

        def prepareEdges(buf: ArrayBuffer[Int]) : (Array[Int], Int) = {
          (removeDuplicates, sortNeighbors) match {
            case (false, false) => (buf.toArray, buf.max)
            case (true, false) =>
              val b = buf.distinct.toArray
              (b, b.max)
            case (false, true) =>
              val b = buf.sorted.toArray
              (b, b(b.length - 1))
            case (true, true) =>
              val b = buf.sorted.distinct.toArray
              (b, b(b.length - 1))
          }
        }

        try {
          val elem = edgesIterator.next()
          val (edges, maxId) = prepareEdges(elem._2)
          NodeIdEdgesMaxId(elem._1, edges, maxId)
        } catch {
          case NonFatal(exc) =>
            throw new IOException("Parsing failed near line: %d in %s"
              .format(lastLineParsed, filename), exc)
        }
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
      graphDir: StoredGraphDir = StoredGraphDir.OnlyOut) =
    new ListOfEdgesGraphReader[Int](directory, prefixFileNames,
      new NodeNumberer.IntIdentity(), ParseString.toInt, removeDuplicates,
      sortNeighbors, separator) {
      override def storedGraphDir: StoredGraphDir = graphDir
    }
}
