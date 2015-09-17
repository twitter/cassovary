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

import org.scalatest.{Matchers, WordSpec}

class ParseStringSpec extends WordSpec with Matchers {

  "ParseString.toInt" should {
    "work on single digit numbers" when {
      val end = 0
      "7" in {
        ParseString.toInt("7", 0, end) shouldEqual 7
      }
      "4" in {
        ParseString.toInt("4", 0, end) shouldEqual 4
      }
    }

    "work on double digit numbers" when {
      "30" in {
        ParseString.toInt("30", 0, 1) shouldEqual 30
        ParseString.toInt("30", 0, 0) shouldEqual 3
      }
    }

    "work on longer numbers" when {
      "12345" in {
        val s = "12345"
        ParseString.toInt(s, 0, 4) shouldEqual 12345
        ParseString.toInt(s, 1, 4) shouldEqual 2345
        ParseString.toInt(s, 0, 2) shouldEqual 123
        ParseString.toInt(s, 2, 4) shouldEqual 345
      }
    }
  }

  "ParseString.toLong" should {
    "work on long numbers" when {
      Seq("12345678900", "1234", "43") foreach { s =>
        ParseString.toLong(s) shouldEqual s.toLong
        ParseString.toLong(s, 0, s.length - 2) shouldEqual s.toLong/10
      }
    }
  }

  "ParseString.identity" should {
    "work on strings" when {
      Seq("abcd", "efg", "x") foreach { s =>
        ParseString.identity(s, 0, s.length - 1) shouldEqual s
        ParseString.identity(s, 0, 0) shouldEqual s.charAt(0).toString
      }
    }
  }

}
