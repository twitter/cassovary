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

import org.junit.runner.RunWith
import org.scalatest.WordSpec
import org.scalatest.junit.JUnitRunner
import org.scalatest.matchers.{ClassicMatchers, ShouldMatchers}


@RunWith(classOf[JUnitRunner])
class SequentialNodeNumbererSpec extends WordSpec with ShouldMatchers with ClassicMatchers {
  "SequentialNodeNumberer" when {
    "numbering Longs" should {
      "use consecutive numbers" in {
        val numberer = new SequentialNodeNumberer[Long]
        numberer.externalToInternal(67L) should be (0)
        numberer.externalToInternal(123L) should be (1)
        numberer.externalToInternal(111L) should be (2)
        numberer.externalToInternal(1L) should be (3)
      }
      "not change when using external id twice" in {
        val numberer = new SequentialNodeNumberer[Long]
        numberer.externalToInternal(67L) should be (0)
        numberer.externalToInternal(123L) should be (1)
        numberer.externalToInternal(67L) should be (0)
        numberer.internalToExternal(1) should be (123L)
      }
      "raise exception when using internal id not defined so far" in {
        val numberer = new SequentialNodeNumberer[Long]
        evaluating (numberer.internalToExternal(4)) should produce [IndexOutOfBoundsException]
      }
    }
    "numbering Strings" should {
      "use consecutive numbers" in {
        val numberer = new SequentialNodeNumberer[String]
        numberer.externalToInternal("bbc") should be (0)
        numberer.externalToInternal("a") should be (1)
        numberer.externalToInternal("ab") should be (2)
        numberer.externalToInternal("ac") should be (3)
      }
      "not change when using external id twice" in {
        val numberer = new SequentialNodeNumberer[String]
        numberer.externalToInternal("bbc") should be (0)
        numberer.externalToInternal("a") should be (1)
        numberer.externalToInternal("bbc") should be (0)
        numberer.internalToExternal(1) should be ("a")
      }
      "raise exception when using internal id not defined so far" in {
        val numberer = new SequentialNodeNumberer[String]
        evaluating (numberer.internalToExternal(4)) should produce [IndexOutOfBoundsException]
      }
    }
  }
}
