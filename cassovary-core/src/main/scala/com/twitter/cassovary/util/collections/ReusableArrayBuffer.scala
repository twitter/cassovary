package com.twitter.cassovary.util.collections

import scala.collection.mutable

/**
 * A reusable array backed buffer containing Ints.
 * Allows reuse via clear() that does not do anything but set an internal variable
 * denoting length to 0.
 *
 * @param initialSize initial size of underlying array
 */
class ReusableArrayBuffer(initialSize: Int = 1024) extends mutable.IndexedSeq[Int] {

  private val GROWTH_FACTOR = 1.5

  private var underlying = new Array[Int](initialSize)
  private var _size = 0

  def length = _size

  def apply(idx: Int): Int = {
    if (idx >= _size) {
      throw new IndexOutOfBoundsException(idx.toString)
    } else {
      underlying(idx)
    }
  }

  def update(idx: Int, elem: Int) {
    if (idx >= length) {
      throw new IndexOutOfBoundsException(idx.toString)
    } else {
      underlying(idx) = elem
    }
  }

  override def foreach[U](f: Int =>  U) {
    for (i <- 0 until length) {
      f(underlying(i))
    }
  }

  /* some more methods added for re-usability */

  def clear(): Unit = {
    _size = 0
  }

  def += (v: Int): Unit = {
    if (underlying.length <= _size) {
      val newSize = (_size * GROWTH_FACTOR).toInt max (_size + 1)
      val old = underlying
      underlying = new Array[Int](newSize)
      scala.compat.Platform.arraycopy(old, 0, underlying, 0, old.length)
    }
    underlying(_size) = v
    _size += 1
  }

  def toArray(offset: Int = 0): Array[Int] = {
    val newSize = _size - offset
    val newArray = new Array[Int](newSize)
    scala.compat.Platform.arraycopy(underlying, offset, newArray, 0, newSize)
    newArray
  }

}
