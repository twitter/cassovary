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

package com.twitter.cassovary.graph.util

import it.unimi.dsi.fastutil.ints.Int2IntMap
import it.unimi.dsi.fastutil.objects.Object2IntMap

object FastUtilConversion {

  def object2IntMapToArray[T](map: Object2IntMap[T]): Array[(T, Int)] = {
    val result = new Array[(T, Int)](map.size)

    val iterator = map.keySet.iterator
    var counter = 0
    while (iterator.hasNext) {
      val key = iterator.next
      result(counter) = (key, map.getInt(key))
      counter += 1
    }

    result
  }

  def int2IntMapToArray(map: Int2IntMap): Array[(Int, Int)] = {
    val result = new Array[(Int, Int)](map.size)

    val iterator = map.keySet.iterator
    var counter = 0
    while (iterator.hasNext) {
      val key = iterator.nextInt
      result(counter) = (key, map.get(key))
      counter += 1
    }

    result
  }

}