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
import org.scalatest.{PrivateMethodTester, WordSpec}
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class BinomialDistributionSpec extends WordSpec with ShouldMatchers with PrivateMethodTester {
  val twoPoint = new BinomialDistribution(1, 0.3)

  val twoTrials = new BinomialDistribution(2, 0.8)

  val distribution10 = new BinomialDistribution(10, 0.4)

  val pdf = PrivateMethod[Array[Double]]('pdf)
  val cdf = PrivateMethod[Array[Double]]('cdf)

  "two point distribution" should {
    "have correct pdf" in {
      (twoPoint invokePrivate pdf())(0) should be (0.7)
      (twoPoint invokePrivate pdf())(1) should be (0.3)
    }
    "have correct cdf" in {
      (twoPoint invokePrivate cdf())(0) should be (0.7)
    }
  }

  "binomial distribution with 2 trials" should {
    val expected = Array(0.2 * 0.2, 2 * 0.2 * 0.8, 0.8 * 0.8)
    "have correct pdf" in {
      (twoTrials invokePrivate pdf()).zip(expected).foreach{case (x, y) => x should be (y plusOrMinus 0.001)}
    }

    "have correct cdf" in {
      val expected = Array(0.2 * 0.2, 0.2 * 0.2 + 2 * 0.2 * 0.8)
      (twoTrials invokePrivate cdf()).zip(expected).foreach{case (x, y) => x should be (y plusOrMinus 0.001)}
    }
  }

  "binomial distribution with 10 trials" should {
    "have non-negative pdf" in {
      (distribution10 invokePrivate pdf()).foreach{
        p => p should be >= 0.0
      }
    }

    "have pdf that sums up to 1" in {
      (distribution10 invokePrivate pdf()).sum should be (1.0 plusOrMinus 0.005)
    }

    "have increasing cdf" in {
      (distribution10 invokePrivate cdf()).foldLeft(0.0){case (a, b) =>
        a should be < b
        b
      }
    }

    "give samples that follow pdf in terms of mse" in {
      val sampleSize = 10000
      val rng = new Random
      val histogram = distribution10.sample(rng).take(sampleSize)
        .groupBy(identity)
        .map{ case (x, y) => (x, y.length.toDouble / sampleSize)}
      var mse = 0.0
      for (i <- 0 to 10) {
        val error = (distribution10 invokePrivate pdf())(i) - histogram.getOrElse(i, 0.0)
        mse += error * error
      }
      mse should be (0.033 plusOrMinus 0.01)
    }
  }
}
