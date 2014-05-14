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
package com.twitter.cassovary.graph2

import org.specs.Specification
import java.nio.file.Files
import scala.util.Random

class SimpleMappedGraphSpec extends Specification {
  val unsortedEdgeMap: Map[Long, Seq[Long]] = (0 until 1000) .map { _ =>
    Random.nextLong() -> ((0 until 10) map { _ => Random.nextLong() })
  }.toMap

  val edges = unsortedEdgeMap.toSeq.sortBy(_._1)

  val tempDir = Files.createTempDirectory("cassovary").toString
  SimpleMappedGraph.write(tempDir, edges.toStream)
  val graph = new SimpleMappedGraph(tempDir)

  "A SimpleMappedGraph" should {
    "group the input stream of edges correctly" in {
      val input = Seq((1, 1), (2, 3), (2, 4)).toStream
      SimpleMappedGraph.collectEdges(input).toList mustEqual List((1, Seq(1)), (2, Seq(3, 4)))
      val input2 = Seq((1, 1), (2, 3), (3, 4)).toStream
      SimpleMappedGraph.collectEdges(input2).toList mustEqual List((1, Seq(1)), (2, Seq(3)), (3, Seq(4)))
    }

    "Retrieve edges correctly" in {
      println(tempDir)
      edges map { case (k: Long, v: Seq[Long]) =>
        graph.neighbors(k).get mustEqual Some(v)
      }
      (0 until 1000) map { _ =>
        val n = Random.nextLong()
        graph.neighbors(n).get mustEqual unsortedEdgeMap.get(n)
      }
    }
  }
}
