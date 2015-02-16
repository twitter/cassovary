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
package com.twitter.cassovary.util.io

import org.scalatest.{Matchers, WordSpec}
import scala.reflect.runtime.universe._

class LabelsReaderSpec extends WordSpec with Matchers {
  val reader = new LabelsReader("cassovary-core/src/test/resources/graphs", "nodelabels_toy1")

  "Int label reader" should {
    "read int array-based labels" in {
      val labels = reader.read(10)
      labels.get[String]("label1") shouldEqual None

      val label = labels.get[Int]("label1")
      label.isDefined shouldEqual true
      label.get.name shouldEqual "label1"
      label.get.tag shouldEqual typeTag[Int]
      label.get.toList.filter(_._2 != 0).sorted shouldEqual List((1,1000), (2, 3333), (10, 2000))
    }
  }

}
