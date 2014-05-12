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
import org.scalatest.matchers.ShouldMatchers
import org.scalatest.WordSpec
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class SamplingSpec extends WordSpec with ShouldMatchers {
  val array = (1 to 100).toArray

  val rng = new Random

  "Sampling" should {
    "sample random subset" which {
      "have correct number of elements" when {
        "sampling subset of size lower than size of the set" in {
          Sampling.randomSubset(5, array, rng).length shouldEqual 5
        }
        "sampling subset of size equal to the size of the set" in {
          Sampling.randomSubset(100, array, rng).length shouldEqual 100
        }
        "sampling subset of size hihger than the size of the set" in {
          Sampling.randomSubset(104, array, rng).size shouldEqual 100
        }
      }
      "does not have duplicates" in {
        Sampling.randomSubset(100, array, rng).toSet.size shouldEqual 100
      }
      "is not biased towards any value" in {
        val positiveSubset = {
          val start = rng.nextInt(100)
          (start to (start + 49)).map(_ % 100 + 1).toSet
        }
        (1 to 100).flatMap(_ => Sampling.randomSubset(10, array, rng))
          .map(x => if (positiveSubset.contains(x)) 1 else 0)
          .sum should (be < 600 and be > 400)
      }
    }
  }
}
