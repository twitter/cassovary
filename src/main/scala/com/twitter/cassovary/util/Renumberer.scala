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
 * Renumber integer ids to integers in increasing order (hereby referred to as indices)
 * Useful when needing to "compact" a list of non-sequential integers
 * Any translations must be on ids >= 1, or the reverse mapping is undefined.
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
   * The renumbered indices start from 1
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
   * Will return 0 if a given id doesn't exist in the index
   * @param index
   */
  def reverseTranslate(index: Int): Int = {
    prepareReverse
    try {
      indexToId(index)
    }
    catch {
      case e: ArrayIndexOutOfBoundsException => 0
    }
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
   * Resize this renumberer
   * @param newMaxId New MaxId to resize to
   */
  def resize(newMaxId: Int) {
    if (newMaxId <= idToIndex.size - 1) {
      throw new IllegalArgumentException("NewMaxId %s <= Old MaxId %s".format(newMaxId, idToIndex.size - 1))
    }
    val newIdToIndex = new Array[Int](newMaxId+1)
    Array.copy(idToIndex, 0, newIdToIndex, 0, idToIndex.size)
    idToIndex = newIdToIndex
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
