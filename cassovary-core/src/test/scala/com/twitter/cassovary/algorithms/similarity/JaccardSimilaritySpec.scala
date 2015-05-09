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
package com.twitter.cassovary.algorithms.similarity

import com.twitter.cassovary.graph.{GraphDir, TestGraphs}
import org.scalatest.{Matchers, WordSpec}

class JaccardSimilaritySpec extends WordSpec with Matchers {

  val EPSILON = 1e-6

  "Jaccard Similarity algorithm" should {

    "return the correct value for OutDir" in {
      val graph = TestGraphs.g6
      val dir = GraphDir.OutDir
      val jaccardSimilarity = new JaccardSimilarity(graph)

      val similarityScore = jaccardSimilarity.calculateSimilarity(dir, 10, 13)
      similarityScore shouldEqual 0.25

      val topKSimilarNodes = jaccardSimilarity.getTopKSimilarNodes(dir, 10, 5)
      topKSimilarNodes shouldEqual Seq((11, 0.25), (13, 0.25), (15, 0.25))

      val topKAllSimilarNodes = jaccardSimilarity.getTopKAllSimilarPairs(dir, 10)
      topKAllSimilarNodes shouldEqual Map(10 -> Seq((11, 0.25), (13, 0.25), (15, 0.25)),
        11 -> Seq((13, 1.0), (12, 0.5), (10, 0.25)),
        12 -> Seq((11, 0.5), (13, 0.5)),
        13 -> Seq((11, 1.0), (12, 0.5), (10, 0.25)),
        14 -> Seq(),
        15 -> Seq((10, 0.25)))
    }

    "return the correct value for InDir" in {
      val graph = TestGraphs.g6_onlyin
      val dir = GraphDir.InDir
      val jaccardSimilarity = new JaccardSimilarity(graph)

      val similarityScore = jaccardSimilarity.calculateSimilarity(dir, 11, 12)
      similarityScore shouldEqual 0.5

      val topKSimilarNodes = jaccardSimilarity.getTopKSimilarNodes(dir, 11, 5)
      topKSimilarNodes shouldEqual Seq((13, 1.0), (12, 0.5), (10, 0.25))

      val topKAllSimilarNodes = jaccardSimilarity.getTopKAllSimilarPairs(dir, 10)
      topKAllSimilarNodes shouldEqual Map(10 -> Seq((11, 0.25), (13, 0.25), (15, 0.25)),
        11 -> Seq((13,1.0), (12, 0.5), (10, 0.25)),
        12 -> Seq((11, 0.5), (13, 0.5)),
        13 -> Seq((11,1.0), (12, 0.5), (10, 0.25)),
        14 -> Seq(),
        15 -> Seq((10, 0.25)))
    }
  }
}
