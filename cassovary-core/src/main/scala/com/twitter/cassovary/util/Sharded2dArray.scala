package com.twitter.cassovary.util

/**
 * A wrapper that behaves as an Array of internal Arrays of `T`.
 *
 * Internally, however, it keeps only a constant number of arrays, called shards.
 *
 * @param shards shards array
 * @param indicator function that is true iff a given inner array is present
 * @param offsets offsets of each inner array in the shards array
 * @param lengths lengths of each inner arrays
 * @param hashing function that maps inner array id to corresponding shard
 */
class Sharded2dArray[@specialized(Int, Long) T](shards: Array[Array[T]],
                                                indicator: T => Boolean,
                                                offsets: T => Int,
                                                lengths: T => Int,
                                                hashing: T => Int) {
  def apply(id: T): Seq[T] = {
    if (indicator(id)) {
      new ArraySlice[T](shards(hashing(id)), offsets(id), lengths(id))
    } else {
      null
    }
  }
}

object Sharded2dArray {
  def fromArrays(shards: Array[Array[Int]], nodeSet: Array[Byte],
                 offsets: Array[Int], lengths: Array[Int]): Sharded2dArray[Int] = {
    new Sharded2dArray[Int](shards, x => nodeSet(x) != 0, x => offsets(x), x => lengths(x),
                            x => x % shards.length)
  }
}
