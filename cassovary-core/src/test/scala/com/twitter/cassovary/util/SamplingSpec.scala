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
  val range =  1 to 100

  val rng = new Random

  case class Method(procedure: (Int => Array[Int]), name: String)
  val methods = Seq(
    Method((num: Int) => Sampling.randomSubset(num, array, rng), "array based"),
    Method((num: Int) => Sampling.randomSubset(num, range, rng), "range based")
  )

  "Sampling" should {
    for (method <- methods) {
      "sample random subset (" + method.name + ")" which {
        "have correct number of elements" when {
          "sampling subset of size lower than size of the set" in {
            method.procedure(5).length shouldEqual 5
          }
          "sampling subset of size equal to the size of the set" in {
            method.procedure(100).length shouldEqual 100
          }
          "sampling subset of size hihger than the size of the set" in {
            method.procedure(104).size shouldEqual 100
          }
        }
        "does not have duplicates" in {
          method.procedure(100).toSet.size shouldEqual 100
        }
        "is not biased towards any value" in {
          val positiveSubset = {
            val start = rng.nextInt(100)
            (start to (start + 49)).map(_ % 100 + 1).toSet
          }

          // In 100 samplings we count how many times we pick a number belonging
          // to '''positiveSet'''. By Hoeffding's inequality this
          // number should between 400 and 600 with probability almost 1.
          (1 to 100).flatMap(_ => method.procedure(10))
            .map(x => if (positiveSubset.contains(x)) 1 else 0)
            .sum should (be < 600 and be > 400)
        }
      }
    }
  }
}
