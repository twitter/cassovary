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

/**
 * Splits a graph read by `graphReaderFromDirectory` to multiple subgraphs, each
 * in a separate subdirectory, named "instance_i" for partition numbered i.
 * Splitting is done as per `partitioner`.
 */
class GraphFilesSplitter[T](outputDir: String, partitioner: Partitioner,
    graphReaderFromDirectory: GraphReaderFromDirectory[T]) {

  private val futurePool = new BoundedFuturePool(FuturePool.unboundedPool,
    graphReaderFromDirectory.parallelismLimit)
  private val log = Logger.get("graphFilesSplitter")

  def splitGraph(): Unit = {
    // there are many parts of the original input graph
    val inputParts = graphReaderFromDirectory.iterableSeq

    // instanceWriters is a 2-D array indexed by input part# and instance#
    val instanceWriters = setupPerInstanceSubdirectories(partitioner.numInstances,
      graphReaderFromDirectory.iterableSeq.length)
    val futures = Future.collect(inputParts.indices map { i =>
      split(inputParts(i).iterator, instanceWriters(i))
    })
    Await.result(futures)
  }

  private def mkDirHelper(dirName: String): Unit = {
    val dir = new File(dirName)
    if (dir.exists()) {
      log.info("Directory %s already exists.", dir)
    } else {
      if (dir.mkdir()) {
        log.debug("Made new directory %s", dir)
      } else {
        throw new FileNotFoundException("Unable to create new directory " + dir)
      }
    }
  }

  private def getBufferedWriter(fileName: String): BufferedWriter = {
    try {
      val f = new File(fileName)
      f.createNewFile()
      new BufferedWriter(new OutputStreamWriter(new FileOutputStream(f), "utf-8"))
    } catch {
      case ex : IOException => throw new IOException(ex.toString)
    }
  }

  // @return an array of arrays. The right index is of subgraph instance number and
  // left index is of input seq number.
  private def setupPerInstanceSubdirectories(numInstances: Int,
      numInputParts: Int): Array[Array[BufferedWriter]] = {
    mkDirHelper(outputDir)
    val instanceWriters = Array.ofDim[BufferedWriter](numInputParts, numInstances)
    (0 until numInstances) foreach { i =>
        val subDirName = outputDir + "/instance_" + i
        mkDirHelper(subDirName)
        (0 until numInputParts) foreach { j =>
          instanceWriters(j)(i) = getBufferedWriter(subDirName + "/" + j)
        }
    }
    instanceWriters
  }

  private def split(it: Iterator[NodeIdEdgesMaxId],
      instanceWriters: Array[BufferedWriter]): Future[Unit] = futurePool {
    it foreach { origNode =>
      partitioner.map(origNode) foreach { case (instance, node) =>
        instanceWriters(instance).write(graphReaderFromDirectory.reverseParseNode(node))
      }
    }
    instanceWriters foreach { writer =>
      writer.flush()
      writer.close()
    }
  }
}
