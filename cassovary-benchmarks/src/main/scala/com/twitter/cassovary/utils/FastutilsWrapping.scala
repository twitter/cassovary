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
package com.twitter.cassovary.utils

import com.twitter.cassovary.util.FastUtilUtils
import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

object FastUtilsWrappingBenchmark {
  def size = 10000000

  def time(name: String)(a: => Unit) = {
    val start = System.currentTimeMillis()
    a
    println(s"time of $name: ${(System.currentTimeMillis() - start)}")
  }

  def main(args: Array[String]): Unit = {
    val easyMap = FastUtilUtils.newInt2IntOpenHashMap()

    val keys = Array.tabulate[Int](size)(identity[Int])

    val values = Array.tabulate[Int](size)(_ + 7)

    val pureMap = new Int2IntOpenHashMap()

    time ("pure map ops") {
      (0 until size).foreach {
        i => pureMap.put(keys(i), values(i))
      }
      (0 until size).foreach {
        i => pureMap.get(keys(i))
      }
    }
    
    time("wrapped map ops") {
      (0 until size).foreach {
        i => easyMap.put(keys(i), values(i))
      }

      (0 until size).foreach {
        i => easyMap.get(keys(i))
      }
    }
  }
}
