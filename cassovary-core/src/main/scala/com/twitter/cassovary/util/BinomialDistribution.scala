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

import java.util.Arrays
import scala.util.Random

class BinomialDistribution(n: Int, p: Double) {
  private lazy val pdf: Array[Double] = calculatePdf()
  private lazy val cdf: Array[Double] = pdf.scanLeft(0.0)(_ + _) drop 1

  private def calculatePdf() = {
    val pdf = Array.fill(n + 1)(0.0)
    pdf(0) = math.pow(1 - p, n)
    for (i <- 1 to n) {
      pdf(i) = p * (n - i + 1) / (i * (1 - p)) * pdf(i - 1)
    }
    pdf
  }

  def sample(rng: Random): Int = p match {
    case 1 => n
    case 0 => 0
    case _ =>
      val unifDouble = rng.nextDouble()
      Arrays.binarySearch(cdf, unifDouble) match {
        case found: Int if found >= 0 => found - 1
        case negatedInsertionPointMinusOne: Int => -negatedInsertionPointMinusOne - 1
      }
  }

  def samplesStream(rng: Random): Stream[Int] = sample(rng) #:: samplesStream(rng)
}