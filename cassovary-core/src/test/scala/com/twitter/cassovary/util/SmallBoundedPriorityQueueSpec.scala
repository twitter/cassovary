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

import scala.reflect.ClassManifest

class SmallBoundedPriorityQueueSpec extends WordSpec with ShouldMatchers {
  def emptyQueue[A](maxSize: Int)(implicit m: ClassManifest[A], ord: Ordering[A]) = {
    new SmallBoundedPriorityQueue[A](maxSize)
  }

  def fullQueue[A](content: List[A])(implicit m: ClassManifest[A], ord: Ordering[A]) = {
    val q = new SmallBoundedPriorityQueue[A](content.length)
    content.foreach(q += _)
    q
  }

  "SmallBoundedPriorityQueue of 3 ints" when {

    "empty" should {
      "return empty list" in {
        val q = emptyQueue[Int](3)
        q.top(0) shouldEqual Seq()
        q.top(1) shouldEqual Seq()
      }
    }

    "not full" should {
      "add elements" in {
        val q = emptyQueue[Int](3)
        q += 4
        q.top(1) shouldEqual Seq(4)
        q += 3
        q.top(2) shouldEqual Seq(4, 3)
        q += 30
        q.top(3) shouldEqual Seq(30, 4, 3)
      }
    }

    "full" should {
      "substitute lowest element correctly" in {
        val q = fullQueue[Int](List(4, 3, 30))
        q += 10
        q.top(3) shouldEqual Seq(30, 10, 4)
        q += 100
        q += 1
        q += 10
        q.top(3) shouldEqual Seq(100, 30, 10)
      }

      "return correct number of elements" in {
        val q = fullQueue[Int](List(4, 3, 30))
        q.top(1) shouldEqual Seq(30)
      }
    }

    "cleared" should {
      "start over" in {
        val q = fullQueue[Int](List(40, 130, 30))
        q.clear()
        q.size shouldEqual 0
        q += 1
        q += 10
        q.top(2) shouldEqual Seq(10, 1)
      }
    }
  }

  "SmallBoundedPriorityQueue of 5 strings" when {

    "not full" should {
      "add elements" in {
        val q = emptyQueue[String](5)
        q += "ab"
        q += "cd"
        q.top(2) shouldEqual Seq("cd", "ab")
        q += "ar"
        q += "be"
        q += "fd"
        q.top(5) shouldEqual Seq("fd", "cd", "be", "ar", "ab")
      }
    }

    "full" should {
      "substitute lowest element correctly" in {
        val q = fullQueue[String](List("fd", "cd", "be", "ar", "ab"))
        q += "zz"
        q += "xx"
        q.top(5) shouldEqual Seq("zz", "xx", "fd", "cd", "be")
      }

      "return correct number of elements" in {
        val q = fullQueue[String](List("ar", "ab", "fd", "cd", "be"))
        q.top(3) shouldEqual Seq("fd", "cd", "be")
      }
    }
  }
}
