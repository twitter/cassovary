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

import java.io.File
import java.util.concurrent.Executors

import com.twitter.cassovary.util.io.AdjacencyListGraphReader
import com.twitter.common.util.FileUtils
import org.scalatest.{Matchers, WordSpec}

class GraphFilesSplitterSpec extends WordSpec with Matchers {
  val inputGraphDir = "cassovary-core/src/test/resources/graphs"
  val reader = AdjacencyListGraphReader.forIntIds(inputGraphDir, "toy_6nodes_adj")
  val tmpDir = "/tmp/test_graph_splitter"
  val numInstances = 2
  val partitioner = new HashSourceMapper(numInstances, i => i % numInstances)
  val splitter = new GraphFilesSplitter[Int](tmpDir, partitioner, reader)
  "splitter" should {
    "make appropriate output files and directories" in {
      splitter.splitGraph()
      val tmpd = new File(tmpDir)
      val subdirs = tmpd.list()
      val expectedSubDirs = (0 until numInstances).map(i => "instance_" + i).toList.sorted
      subdirs.toList.sorted shouldEqual expectedSubDirs
      val expectedFiles = (0 until reader.iterableSeq.length).map(_.toString).toList.sorted
      subdirs foreach { s =>
        val files = new File(tmpDir + "/" + s).list()
        files.toList.sorted shouldEqual expectedFiles
      }
      FileUtils.forceDeletePath(tmpd)
    }
  }
}
