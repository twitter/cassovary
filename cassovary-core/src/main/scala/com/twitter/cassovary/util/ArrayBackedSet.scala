package com.twitter.cassovary.util

/**
 * A set backed by an underlying array that keeps track of ints in the range 0..maxId both
 * inclusive. Updates and reads can be used concurrently as each set element has its own
 * location in the array.
 *
 * @param maxVal maximum integer value in the set
 */

class ArrayBackedSet(val maxVal: Int) {
  private val underlying = new Array[Byte](maxVal + 1)

  // note that add and remove are idempotent and can be done as many times

  def add(i: Int) = { underlying(i) = 1 }

  def remove(i: Int) = { underlying(i) = 0 }

  def contains(i: Int) = underlying(i) == 1

  def iterator = (0 to maxVal).filter( i => underlying(i) != 0).iterator

  // this is expensive as it iterates through the whole set, so use carefully
  def size = iterator.size

}
