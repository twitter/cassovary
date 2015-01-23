package com.twitter.cassovary.util

import java.{util => jutil}

object SortedArrayOps {
  /**
   * @return true if `elem` is in the `array`.
   *
   * Assumes that the collection is sorted. Does not use built-in Scala's
   * searching methods to avoid boxing/unboxing.
   */
  def exists(array: Array[Int], elem: Int): Boolean = {
    jutil.Arrays.binarySearch(array, elem) match {
      case idx if idx < 0 => false
      case idx => true
    }
  }

  /**
   * Linear intersection of two sorted arrays.
   */
  def intersectSorted(array: Array[Int], that: Array[Int]): Array[Int] = {
    val intersection = Array.ofDim[Int](math.min(array.length, that.length))
    var intersectionIdx = 0
    var arrayIdx = 0
    var thatIdx = 0

    while (arrayIdx < array.length && thatIdx < that.length) {
      if (array(arrayIdx) == that(thatIdx)) {
        intersection(intersectionIdx) = array(arrayIdx)
        intersectionIdx += 1
        arrayIdx += 1
        thatIdx += 1
      } else if (array(arrayIdx) < that(thatIdx)) {
        arrayIdx += 1
      } else {
        thatIdx += 1
      }
    }

    slice(intersection, 0, intersectionIdx)
  }

  /**
   * Linear union of two sorted arrays.
   *
   * @return A sorted array containing only once each element
   *         that wast in either of two arrays.
   */
  def unionSorted(thisA: Array[Int], that: Array[Int]): Array[Int] = {
    val union = Array.ofDim[Int](thisA.length + that.length)
    var resultIdx = 0
    var thisIdx = 0
    var thatIdx = 0

    @inline def putInDest(x: Int): Unit = {
      union(resultIdx) = x
      resultIdx += 1
    }

    while (thisIdx < thisA.length || thatIdx < that.length) {
      if (thisIdx == thisA.length) {
        putInDest(that(thatIdx))
        thatIdx += 1
      } else if (thatIdx == that.length) {
        putInDest(thisA(thisIdx))
        thisIdx += 1
      } else {
        if (thisA(thisIdx) == that(thatIdx)) {
          putInDest(thisA(thisIdx))
          thisIdx += 1
          thatIdx += 1 // ignoring duplicates
        } else if (thisA(thisIdx) < that(thatIdx)) {
          putInDest(thisA(thisIdx))
          thisIdx += 1
        } else {
          putInDest(that(thatIdx))
          thatIdx += 1
        }
      }
    }

    slice(union, 0, resultIdx)
  }

  @inline def slice(a: Array[Int], from: Int, len: Int): Array[Int] = {
    val result = Array.ofDim[Int](len)
    Array.copy(a, from, result, 0, len)
    result
  }
}

