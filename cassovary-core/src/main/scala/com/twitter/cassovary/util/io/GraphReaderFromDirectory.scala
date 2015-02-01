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

import com.twitter.cassovary.graph.NodeIdEdgesMaxId
import java.io.File


/**
 * A subtrait of GraphReader that reads files of names specified by prefix
 * and containing directory.
 */
trait GraphReaderFromDirectory[T] extends GraphReader[T] {
  /**
   * Directory name to read files from
   */
  def directory : String

  /**
   * Filenames prefix
   */
  def prefixFileNames : String = ""

  /**
   * Returns a reader for a given file (shard).
   */
  def oneShardReader(filename : String) : Iterable[NodeIdEdgesMaxId]

  /**
   * Should return a sequence of `NodeIdEdgesMaxId` iterables
   */
  def iterableSeq: Seq[Iterable[NodeIdEdgesMaxId]] = {
    val dir = new File(directory)
    val filesInDir = dir.list()
    if (filesInDir == null) {
      throw new Exception("Current directory is " + System.getProperty("user.dir") +
        " and nothing was found in dir " + dir)
    }
    val validFiles = filesInDir.flatMap({ filename =>
      if (filename.startsWith(prefixFileNames)) {
        Some(filename)
      }
      else {
        None
      }
    })
    validFiles.map { filename =>
      oneShardReader(directory + "/" + filename)
    }
  }
}
