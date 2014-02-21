/*
 * Copyright 2014 Twitter, Inc.
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

/**
 * This class is a wrapper around a "shared array" to provide the Seq trait functions
 * the concept of shared array is that a number of Seqs share a two-dimensional big array
 * as the internal storage, its first dimension index is based on 'id' mod the number of shards,
 * then the second dimension array is indexed by 'offset' and of length 'length'
 * @param id an id of the seq, determines the shard number of the first dimensional
 * array in internal storage
 * @param sharedArray a two-dimensional array that holds the underlying data which
 * the SharedArraySeq points to
 * @param offset the offset in the second dimension
 * @param length length of the Seq
 */
class SharedArraySeq(id: Int, sharedArray: Array[Array[Int]], offset: Int, override val length: Int)
    extends IndexedSeq[Int] {
  val sharedArrayIdx = id % sharedArray.length

  def apply(idx: Int): Int = {
    sharedArray(sharedArrayIdx)(offset + idx)
  }

  def update(idx: Int, elem: Int) {
    sharedArray(sharedArrayIdx)(offset + idx) = elem
  }

  override def foreach[U](f: Int =>  U) {
    for (i <- 0 until length) {
      f(sharedArray(sharedArrayIdx)(offset + i))
    }
  }
}
