/*
 * Copyright 2013 Twitter, Inc.
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
import java.io.File


/**
 * A subtrait of GraphReader that reads files of names specified by prefix
 * and containing directory.
 */
trait GraphReaderFromDirectory extends GraphReader {
  /**
   * Directory name to read files from
   */
  def directory : String

  /**
   * Filenames prefix
   */
  def prefixFileNames : String

  /**
   * Returns a reader for a given file (shard).
   */
  def oneShardReader(filename : String) : Iterator[NodeIdEdgesMaxId]

  /**
   * Read in nodes and edges from multiple files. Use shardReaderCompanion
   * for reading a single file.
   * @param directory Directory to read from
   * @param prefixFileNames the string that each part file starts with
   */
  class ShardsReader(directory: String, prefixFileNames: String = "",
                     nodeRenumberer : NodeRenumberer) {
    val dir = new File(directory)

    def readers: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
      val validFiles = dir.list().flatMap({ filename =>
        if (filename.startsWith(prefixFileNames)) {
          Some(filename)
        }
        else {
          None
        }
      })
      validFiles.map({ filename =>
      {() => oneShardReader(directory + "/" + filename)}
      }).toSeq
    }
  }

  /**
   * Should return a sequence of iterators over NodeIdEdgesMaxId objects
   */
  override def iteratorSeq: Seq[() => Iterator[NodeIdEdgesMaxId]] = {
    new ShardsReader(directory, prefixFileNames, nodeRenumberer).readers
  }
}
