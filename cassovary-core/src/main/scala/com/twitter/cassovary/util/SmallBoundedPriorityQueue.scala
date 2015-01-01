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

import scala.reflect.ClassManifest

/**
 * Represents a priority queue that is bounded by a small maximum (e.g., < 10 elements).
 * The items are simply sorted when needed to be in priority order.
 */
class SmallBoundedPriorityQueue[A](maxSize: Int)(implicit m: ClassManifest[A], ord: Ordering[A]) {
import ord._

  private val items = new Array[A](maxSize)
  private var numItems = 0

  // the current minimum value
  private var minItem: A = _

  // index in items where the minItem resides
  private var posMin = 0

  /**
   * Adds an element to the queue
   */
  def +=(elem: A): Unit = {
    if (numItems < maxSize) {
      items(numItems) = elem
      numItems += 1
    } else {
      // calculate current minimum and replace it with the new element if needed
      calcMin()
      if (elem > minItem) {
        items(posMin) = elem
      }
    }
  }

  private def calcMin() = {
    posMin = 0
    minItem = items(0)
    (1 until numItems) foreach { i =>
      if (items(i) < minItem) {
        posMin = i
        minItem = items(i)
      }
    }
  }

  /**
   * Number of elements in the queue.
   */
  def size = numItems

  /**
   * Returns the top {@code k} elements.
   */
  def top(k: Int): Seq[A] = {
    items.take(numItems).toList.sortWith(_ > _).take(k)
  }

  def clear() {
    posMin = 0
    numItems = 0
  }

}
