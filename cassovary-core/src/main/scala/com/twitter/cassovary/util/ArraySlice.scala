/*
 * Copyright 2015 Twitter, Inc.
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

import scala.collection.mutable

/**
 * This class is a wrapper around a "shared array" to provide the Seq trait functions.
 *
 * The concept of shared array is that a number of Seqs share an array
 * as the internal storage. Each of the arrays is defined by its offset from the beginning
 * of the shared array and length.
 * @param array wrapped array
 * @param offset the offset in the `array`
 * @param length length of the Seq
 */
class ArraySlice[@specialized(Int, Long) T](array: Array[T],
                                            offset: Int, override val length: Int)
    extends mutable.IndexedSeq[T] {

  def apply(idx: Int): T = {
    if (idx >= length) {
      throw new IndexOutOfBoundsException(idx.toString)
    } else {
      array(offset + idx)
    }
  }

  def update(idx: Int, elem: T) {
    if (idx >= length) {
      throw new IndexOutOfBoundsException()
    } else {
      array(offset + idx) = elem
    }
  }

  override def foreach[U](f: T =>  U) {
    for (i <- 0 until length) {
      f(array(offset + i))
    }
  }
}
