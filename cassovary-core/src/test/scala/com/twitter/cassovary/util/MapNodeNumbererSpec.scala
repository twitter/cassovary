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
import org.scalatest.junit.JUnitRunner
import org.scalatest.WordSpec
import org.scalatest.matchers.{ClassicMatchers, ShouldMatchers}

@RunWith(classOf[JUnitRunner])
class MapNodeNumbererSpec extends WordSpec with ShouldMatchers with ClassicMatchers {
  val directory = "cassovary-core/src/test/resources/nodeNumberers/"

  "Map node numberer" should {
    "load correctly from the file" in {
      val numberer = MapNodeNumberer.forStringsFromFile(directory + "numbering1.txt")
      numberer.externalToInternal("banana") should be (2)
      numberer.externalToInternal("orange") should be (7)
      numberer.externalToInternal("spoon") should be (29)
      numberer.externalToInternal("bike") should be (12)

      numberer.internalToExternal(2) should be ("banana")
      numberer.internalToExternal(7) should be ("orange")
      numberer.internalToExternal(29) should be ("spoon")
      numberer.internalToExternal(12) should be ("bike")

      evaluating (numberer.externalToInternal("apple")) should produce [NoSuchElementException]

      evaluating (numberer.internalToExternal(3)) should produce [NoSuchElementException]
    }

    "find duplicated names" in {
      evaluating (MapNodeNumberer.forStringsFromFile(directory + "numbering-duplicatedId.txt")) should
        produce [AssertionError]
    }

    "find duplicated ids" in {
      evaluating (MapNodeNumberer.forStringsFromFile(directory + "numbering-duplicatedName.txt")) should
        produce [AssertionError]
    }
  }
}
