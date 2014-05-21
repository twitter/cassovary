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

import it.unimi.dsi.fastutil.ints.IntOpenHashSet
import scala.collection.JavaConversions._
import scala.util.Random

object Sampling {
  /**
   * O(size) time algorithm for random subset of a given range known as
   * Fischer-Yates shuffle.
   *
   * If ```size > from.size``` returns all elements.
   *
   * Be aware, that this method changes the ```from``` Array.
   */
  def randomSubset[@specialized(Int) A](size: Int, from: Array[A], rng: Random): Array[A] = {
    if (size > from.size) {
      from
    } else {
      (0 until size).foreach {
        i =>
          val swapIndex = rng.nextInt(from.size - i) + i
          val temp = from(i)
          from(i) = from(swapIndex)
          from(swapIndex) = temp
      }
      from.slice(0, size)
    }
  }

  /**
   * Expected ```O(size)``` time algorithm for random subset of a given range.
   *
   * If ```size > from.size``` returns all elements.
   */
  def randomSubset(elements: Int, range: Range, rng: Random): Array[Int] = {
    if (elements >= range.size) {
      range.toArray
    } else {
      if (elements > range.size / 2) {
        val complement = new IntOpenHashSet(randomSubset(range.size - elements, range, rng).toIterator)
        range.filterNot(complement.contains).toArray
      } else {
        val result = new IntOpenHashSet()
        while (result.size < elements) {
          val randomElement = range(rng.nextInt(range.size))
          result.add(randomElement)
        }
        result.toIntArray
      }
    }
  }
}
