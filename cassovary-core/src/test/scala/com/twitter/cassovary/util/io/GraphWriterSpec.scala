/*
 * Copyright 2013 Twitter, Inc.
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

import com.twitter.cassovary.graph.TestGraphs
import java.io.StringWriter
import org.scalatest.{Matchers, WordSpec}

class GraphWriterSpec extends WordSpec with Matchers {
  val sortNodes = true

  "Graph writer" should {
    "write graph to single file" in {
      val graph = TestGraphs.g6
      val stringWriter = new StringWriter()
      GraphWriter.writeDirectedGraph(graph, stringWriter, sortNodes)
      val expectedResult =
        """10 3
          |11
          |12
          |13
          |11 2
          |12
          |14
          |12 1
          |14
          |13 2
          |12
          |14
          |14 1
          |15
          |15 2
          |10
          |11
          |""".stripMargin
      stringWriter.toString should be (expectedResult)
    }
    "write graph to multiple chunks" when {
      "number of nodes is divisible by the number of chunks" in {
        val graph = TestGraphs.g6
        val stringWriter1 = new StringWriter()
        val stringWriter2 = new StringWriter()
        GraphWriter.writeDirectedGraph(graph, Seq(stringWriter1, stringWriter2), sortNodes)
        stringWriter1.toString should be
          """10 3
            |11
            |12
            |13
            |11 2
            |12
            |14
            |12 1
            |14
            |""".stripMargin
        stringWriter2.toString should be
          """13 2
            |12
            |14
            |14 1
            |15
            |15 2
            |10
            |11
            |""".stripMargin
      }
      "number of nodes is not divisible by the number of chunks" in {
        val graph = TestGraphs.g6
        val writers = Array.fill(5)(new StringWriter())
        GraphWriter.writeDirectedGraph(graph, writers, sortNodes)
        writers(0).toString should be
          """10 3
            |11
            |12
            |13
            |11 2
            |12
            |14
            |""".stripMargin
        writers(1).toString should be
          """12 1
            |14
            |""".stripMargin
        writers(2).toString should be
          """13 2
            |12
            |14
            |""".stripMargin
        writers(3).toString should be
          """14 1
            |15
            |""".stripMargin
        writers(4).toString should be
          """15 2
            |10
            |11
            |""".stripMargin }
    }

  }
}