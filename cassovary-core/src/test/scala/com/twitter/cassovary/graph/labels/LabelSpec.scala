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

package com.twitter.cassovary.graph.labels

import org.scalatest.{Matchers, WordSpec}

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

class LabelSpec extends WordSpec with Matchers {

  def makeMapBasedLabel[L : TypeTag]() = new MapBasedLabel[Int, L]("map based label")

  def behaveLikeMap[L](label: Label[Int, L], f: Int => L, checkMissing: Boolean = true): Unit = {
    label += 1 -> f(1)
    label += 2 -> f(2)
    label.get(1) shouldEqual Some(f(1))
    label.get(2) shouldEqual Some(f(2))
    if (checkMissing) {
      label.get(3) shouldEqual None
      label.iterator.toList.sortBy(_._1) shouldEqual List((1, f(1)), (2, f(2)))
      label -= 1
      label.get(1) shouldEqual None
      label.get(2) shouldEqual Some(f(2))
      label.get(3) shouldEqual None
      label.iterator.toList.sortBy(_._1) shouldEqual List((2, f(2)))
    }
  }

  "label of int ids" should {
    "behave as map when map based" in {
      behaveLikeMap(makeMapBasedLabel[String](), _.toString())
      behaveLikeMap(makeMapBasedLabel[Int](), _ + 1)
    }

    "behave as map when array based and partial" in {
      val label = new ArrayBasedPartialLabel[Int]("partial array-based", 10)
      behaveLikeMap(label, _ + 2)
    }

    "behave as map when array based and not partial" in {
      val label = new ArrayBasedLabel[Int]("all array-based", 10)
      behaveLikeMap(label, _ * 2, checkMissing = false)
    }

    "behave as expected with flag labels concerned with only which ids are present" in {
      val label = new FlagLabel[Int]("flag", mutable.HashSet.empty[Int])
      behaveLikeMap(label, _ => true)
    }

    "behave as a flag label when backed by bitset" in {
      val label = new BitSetBasedFlagLabel("bitFlag", 1000)
      behaveLikeMap(label, _ => true)
    }
  }

  "labels" should {
    "act like a map of string to label" in {
      val label = makeMapBasedLabel[String]()
      val labels = new Labels[Int]
      labels += label
      labels.get[String](label.name) shouldEqual Some(label)
      labels.get[Int](label.name) shouldEqual None
      labels -= label.name
      labels.get[String](label.name) shouldEqual None
    }
  }

}
