package com.twitter.cassovary.util

/**
 * Renumbers ids to integers (hereby referred to as indices) in increasing order
 * Useful when needing to "compact" a list of non-sequential integers
 * @param maxId
 */
class Renumberer(var maxId: Int) {
  private var indexToId = new Array[Int](maxId+1)
  private var index = 0

  /**
   * Translates a given id to a unique identifying index
   * For any Renumberer object, the same id always returns the same index
   * This isn't the case for different Renumberer objects
   * @param id id to map/translate
   * @return index corresponding to id
   */
  def translate(id: Int): Int = synchronized {
    if (indexToId(id) == 0) {
      index += 1
      indexToId(id) = index
      index
    }
    else {
      indexToId(id)
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
   * Warning - very very slow! O(n)!!!
   * @param index
   */
  def reverseTranslate(index: Int): Int = {
    var found = 0
    (1 until maxId).foreach { i =>
      if (indexToId(i) == index) {
        found = i
      }
    }
    if (found > 0) {
      found
    }
    else {
      throw new IllegalArgumentException("This index %s doesn't exist!".format(index))
    }
  }

  /**
   * Save this renumberer to a LoaderSerializerWriter
   * @param writer
   */
  def toWriter(writer:LoaderSerializerWriter) {
    writer.arrayOfInt(indexToId)
    writer.int(index)
  }

  /**
   * Load a renumberer from a LoaderSerializerReader
   * Note that this doesn't allow you to recover the reverse mapping
   * @param reader
   */
  def fromReader(reader:LoaderSerializerReader) {
    indexToId = reader.arrayOfInt()
    maxId = indexToId.size - 1
    index = reader.int
  }
}
