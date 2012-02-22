/*
 * Copyright 2012 Twitter, Inc.
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

import org.specs.Specification

class SmallBoundedPriorityQueueSpec extends Specification {
  "queue of 3 ints" in {
    val q = new SmallBoundedPriorityQueue[Int](3)
    q += 4
    q.top(1).toList mustEqual List(4)
    q += 3
    q.top(2).toList mustEqual List(4, 3)
    q += 30
    q.top(3).toList mustEqual List(30, 4, 3)
    q += 10
    q.top(3).toList mustEqual List(30, 10, 4)
    q += 100
    q += 1
    q += 10
    q.top(3).toList mustEqual List(100, 30, 10)
    q.clear()
    q.size mustEqual 0
    q += 1
    q += 10
    q.top(2).toList mustEqual List(10, 1)
  }

  "queue of 5 strings" in {
    val q = new SmallBoundedPriorityQueue[String](5)
    q += "ab"
    q += "cd"
    q.top(2).toList mustEqual List("cd", "ab")
    q += "ar"
    q += "be"
    q += "fd"
    q.top(5).toList mustEqual List("fd", "cd", "be", "ar", "ab")
    q += "zz"
    q += "xx"
    q.top(5).toList mustEqual List("zz", "xx", "fd", "cd", "be")
  }
}
