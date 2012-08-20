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
package com.twitter.cassovary.util

import net.lag.logging.Logger

/**
 * Renumbers ids to integers (hereby referred to as indices) in increasing order
 * Useful when needing to "compact" a list of non-sequential integers
 * @param maxId
 */
class Renumberer(var maxId: Int) {
  private var idToIndex = new Array[Int](maxId+1)
  private var indexToId: Array[Int] = null
  private var index = 0
  private val log = Logger.get("Renumberer")

  /**
   * Translates a given id to a unique identifying index
   * For any Renumberer object, the same id always returns the same index
   * This isn't the case for different Renumberer objects
   * @param id id to map/translate
   * @return index corresponding to id
   */
  def translate(id: Int): Int = synchronized {
    if (idToIndex(id) == 0) {
      index += 1
      idToIndex(id) = index
      index
    }
    else {
      idToIndex(id)
    }
  }

  /**
   * Number of unique translations done
   */
  def count = index

  /**
   * Given an array of ids, return the corresponding array of indices
   * @param ary input array of ids
   * @return array of corresponding indices
   */
  def translateArray(ary: Array[Int]): Array[Int] = {
    ary.map({ elt => translate(elt) })
  }

  /**
   * Given an index, return the id
   * The initial query may take some time as the reverse index is lazily built
   * @param index
   */
  def reverseTranslate(index: Int): Int = {
    prepareReverse
    indexToId(index)
  }

  /**
   * (Re)build the reverse index whenever the forward index is updated
   */
  def prepareReverse {
    if (indexToId == null || indexToId.size != index + 1) {
      log.info("Rebuilding reverse index...")
      indexToId = new Array[Int](index + 1)
      var j = 0
      idToIndex.foreach { i =>
        indexToId(i) = j
        j += 1
      }
      log.info("Finished rebuilding")
    }
  }

  /**
   * Save this renumberer to a LoaderSerializerWriter
   * @param writer
   */
  def toWriter(writer:LoaderSerializerWriter) {
    writer.arrayOfInt(idToIndex)
    writer.int(index)
  }

  /**
   * Load a renumberer from a LoaderSerializerReader
   * Note that this doesn't allow you to recover the reverse mapping
   * @param reader
   */
  def fromReader(reader:LoaderSerializerReader) {
    idToIndex = reader.arrayOfInt()
    maxId = idToIndex.size - 1
    index = reader.int
  }
}
