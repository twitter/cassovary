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

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers

class SmallBoundedPriorityQueueSpec extends WordSpec with ShouldMatchers {
  "SmallBoundedPriorityQueue of 3 ints" when {
    val q = new SmallBoundedPriorityQueue[Int](3)

    "empty" should {
      "return empty list" in {
        q.top(0).toList shouldEqual List()
        q.top(1).toList shouldEqual List()
      }
    }

    "not full" should {
      "add elements" in {
        q += 4
        q.top(1).toList shouldEqual List(4)
        q += 3
        q.top(2).toList shouldEqual List(4, 3)
        q += 30
        q.top(3).toList shouldEqual List(30, 4, 3)
      }
    }

    "full" should {
      "substitute lowest element correctly" in {
        q += 10
        q.top(3).toList shouldEqual List(30, 10, 4)
        q += 100
        q += 1
        q += 10
        q.top(3).toList shouldEqual List(100, 30, 10)
      }

      "return correct number of elements" in {
        q.top(1).toList shouldEqual List(100)
      }
    }

    "cleared" should {
      "start over" in {
        q.clear()
        q.size shouldEqual 0
        q += 1
        q += 10
        q.top(2).toList shouldEqual List(10, 1)
      }
    }
  }

  "SmallBoundedPriorityQueue of 5 strings" when {
    val q = new SmallBoundedPriorityQueue[String](5)

    "not full" should {
      "add elements" in {
        q += "ab"
        q += "cd"
        q.top(2).toList shouldEqual List("cd", "ab")
        q += "ar"
        q += "be"
        q += "fd"
        q.top(5).toList shouldEqual List("fd", "cd", "be", "ar", "ab")
      }
    }

    "full" should {
      "substitute lowest element correctly" in {
        q += "zz"
        q += "xx"
        q.top(5).toList shouldEqual List("zz", "xx", "fd", "cd", "be")
      }

      "return correct number of elements" in {
        q.top(3).toList shouldEqual List("zz", "xx", "fd")
      }
    }
  }
}
