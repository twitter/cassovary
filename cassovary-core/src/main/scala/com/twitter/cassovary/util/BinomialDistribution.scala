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

import scala.util.Random

class BinomialDistribution(n: Int, p: Double) {
  lazy val pdf : Array[Double] = calculatePdf()
  lazy val cdf : Array[Double] = calculateCdf()

  def calculatePdf() = {
    val pdf = Array.fill(n + 1)(0.0)
    pdf(0) = math.pow(1 - p, n)
    for (i <- 1 to n) {
      pdf(i) = p * (n - i + 1) / (i * (1 - p)) * pdf(i - 1)
    }
    pdf
  }

  def calculateCdf() : Array[Double] = {
    val cdf = Array.fill(n)(0.0)
    cdf(0) = pdf(0)
    for (i <- 1 to n - 1){
      cdf(i) = cdf(i - 1) + pdf(i)
    }
    cdf
  }

  def sample(samples : Int, rng : Random) : Array[Int] = {
    (0 until samples).map{
      _ =>
        val unifDouble = rng.nextDouble()
        math.abs(java.util.Arrays.binarySearch(cdf, unifDouble))
    }.toArray
  }

  def sample(rng : Random) : Int = sample(1, rng)(0)
}
