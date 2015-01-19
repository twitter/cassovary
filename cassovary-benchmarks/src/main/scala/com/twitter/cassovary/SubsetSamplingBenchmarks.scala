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
package com.twitter.cassovary

import com.twitter.app.Flags
import com.twitter.cassovary.util.Sampling
import scala.util.Random

object SubsetSamplingBenchmarks extends App {
  val DEFAULT_REPS = 1000

  val rng = new Random

  val flags = new Flags("Graph generation benchmarks")
  val numFlag = flags("n", 10000, "Sampling from 1..n")
  val subsetSizeFlag = flags("s", 100, "Sampling subset size")
  val repsFlag = flags("reps", DEFAULT_REPS, "Number of times to run benchmark")
  val helpFlag = flags("h", false, "Print usage")
  flags.parseArgs(args)

  if (helpFlag()) {
    println(flags.usage)
  } else {
    performBenchmarks(subsetSizeFlag(), numFlag(), repsFlag())
  }

  abstract case class SubsetSamplingBenchmark(size: Int, maxElement: Int)
    extends OperationBenchmark

  def performBenchmarks(size: Int, maxElement: Int, reps: Int) {
    val benchmarks = List(
      new SubsetSamplingBenchmark(size, maxElement) {
        override def name = "Array based sampling with sampling array generated in advance"
        val array = (1 to maxElement).toArray
        override def operation(): Unit = {
          Sampling.randomSubset(size, array, rng)
        }
      },
      new SubsetSamplingBenchmark(size, maxElement) {
        override def name = "Array based sampling with array generation for each sample"
        override def operation(): Unit = {
          Sampling.randomSubset(size, (1 to maxElement).toArray, rng)
        }
      },
      new SubsetSamplingBenchmark(size, maxElement) {
        override def name = "Range based sampling"
        override def operation(): Unit = {
          Sampling.randomSubset(size, 1 to maxElement, rng)
        }
      }
    )
    for (benchmark <- benchmarks) {
      printf("Sampling %d from 1..%d using %s\n", benchmark.size, benchmark.maxElement, benchmark.name)
      val duration = benchmark.run(reps)
      printf("\tAvg time over %d repetitions: %s.\n", reps, duration)
    }
  }
}
