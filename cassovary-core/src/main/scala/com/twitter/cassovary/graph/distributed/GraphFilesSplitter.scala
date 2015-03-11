/*
 * Copyright 2015 Twitter, Inc.
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
package com.twitter.cassovary.graph.distributed

import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import com.twitter.cassovary.util.BoundedFuturePool
import com.twitter.cassovary.util.io.GraphReaderFromDirectory
import com.twitter.logging.Logger
import com.twitter.util.{Await, Future, FuturePool}
import java.io._

class GraphFilesSplitter[T](outputDir: String, partitioner: Partitioner,
    graphReaderFromDirectory: GraphReaderFromDirectory[T]) {

  private val futurePool = new BoundedFuturePool(FuturePool.unboundedPool,
    graphReaderFromDirectory.parallelismLimit)
  private val log = Logger.get()

  def splitGraph(): Unit = {
    // there are many parts of the original input graph
    val inputParts = graphReaderFromDirectory.iterableSeq

    // writers is a 2-D array indexed by input part# and instance#
    val writers = setupOutputDirectories(partitioner.numInstances,
      graphReaderFromDirectory.iterableSeq.length)

    val futures = Future.collect(inputParts.indices map { i =>
      split(inputParts(i).iterator, writers(i))
    })
    Await.result(futures)
  }

  // @return an array of arrays. The right index is of subgraph instance number and
  // left index is of input seq number.
  private def setupOutputDirectories(numInstances: Int,
      numInputParts: Int): Array[Array[BufferedWriter]] = {
    val dir = new File(outputDir)
    if (dir.exists()) {
      log.info("Directory %s already exists.", dir)
    } else {
      if (dir.mkdir()) {
        log.info("Made new directory %s", dir)
      } else {
        throw new FileNotFoundException("Unable to create new directory " + dir)
      }
    }

    val writers = Array.ofDim[BufferedWriter](numInstances, numInputParts)
    (0 until numInstances) foreach { i =>
      try {
        val subDirName = outputDir + "/instance_" + i
        val subdir = new File(subDirName)
        subdir.mkdir()
        (0 until numInputParts) foreach { j =>
          val fileName = subDirName + "/" + j
          val f = new File(fileName)
          f.createNewFile()
          writers(j)(i) = new BufferedWriter(new OutputStreamWriter(
                    new FileOutputStream(f), "utf-8"))
        }
      } catch {
        case ex : IOException => throw new IOException(ex.toString)
      }
    }
    writers
  }

  private def split(it: Iterator[NodeIdEdgesMaxId],
      writers: Array[BufferedWriter]): Future[Unit] = futurePool {
    it foreach { partitioner.map(_) foreach { case (instance, node) =>
        val writer = writers(instance)
        writer.write(graphReaderFromDirectory.reverseParseNode(node))
      }
    }
    writers foreach { writer =>
      writer.flush()
      writer.close()
    }
  }
}
