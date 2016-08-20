package com.twitter.cassovary.collections

object SortedCSeqOps {
  /**
   * @return true if `elem` is in the `array`.
   *
   * Assumes that the collection is sorted. Does not use built-in Scala's
   * searching methods to avoid boxing/unboxing.
   */
  def exists(cseq: CSeq[Int], elem: Int): Boolean = {
    if (cseq.isEmpty) return false

    def cSeqBinarySearch(a: CSeq[Int], key: Int): Int = {
      var low: Int = 0
      var high: Int = cseq.length - 1

      while (low <= high) {
        val mid: Int = (low + high) >>> 1
        val midVal: Int = a(mid)
        if (midVal < key) low = mid + 1
        else if (midVal > key) high = mid - 1
        else return mid
      }
      return -(low + 1)
    }

    cSeqBinarySearch(cseq, elem) match {
      case idx if idx < 0 => false
      case idx => true
    }
  }

  /**
   * Linear intersection of two sorted Cseqs.
   */
  def intersectSorted(array: CSeq[Int], that: CSeq[Int])(implicit csf: CSeqFactory[Int]):
      CSeq[Int] = {
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

    // We copy common elements to a new array to save memory. Alternatively,
    // we can save processor time and return `intersection` array wrapped in CSeq
    // at the cost of memory leak since the elements of the array after `intersectionIdx`
    // will never be used.
    val resArray = Array.ofDim[Int](intersectionIdx)
    Array.copy(intersection, 0, resArray, 0, intersectionIdx)

    CSeq[Int](resArray)
  }

  /**
   * Linear union of two sorted arrays.
   *
   * @return A sorted array containing only once each element
   *         that wast in either of two arrays.
   */
  def unionSorted(thisA: CSeq[Int], that: CSeq[Int])(implicit csf: CSeqFactory[Int]): CSeq[Int] = {
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

    // Same tradeoff as in intersectionSorted method.
    val resArray = Array.ofDim[Int](resultIdx)
    Array.copy(union, 0, resArray, 0, resultIdx)

    CSeq[Int](resArray)
  }
}

