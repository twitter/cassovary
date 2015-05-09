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
import org.scalatest.matchers.{Matcher, MatchResult}

class CosineSimilaritySpec extends WordSpec with Matchers {

  val EPSILON = 1e-6

  def almostEqual(leftOp: Seq[(Int, Double)], rightOp: Seq[(Int, Double)]) = {
    (leftOp zip rightOp) forall { case (l, r) => l._1 == r._1 && math.abs(l._2 - r._2) < EPSILON}
  }

  def almostEqualSeq(rightOp: Seq[(Int, Double)]) = new Matcher[Seq[(Int, Double)]] {
    def apply(leftOp: Seq[(Int, Double)]) =
      MatchResult(
        almostEqual(leftOp, rightOp),
        "Similarity Score isn't equal! \nExpected: %s \nActual: %s".format(
          rightOp.mkString(", "), leftOp.mkString(", ")),
        "Similarity Scores are equal"
      )
  }

  def almostEqualMap(rightOp: Map[Int, Seq[(Int, Double)]]) = new Matcher[Map[Int, Seq[(Int, Double)]]] {
    def apply(leftOp: Map[Int, Seq[(Int, Double)]]) =
      MatchResult(
        rightOp forall { case (i, d) => almostEqual(d, leftOp(i)) },
        "Similarity Score isn't equal! \nExpected: %s \nActual: %s".format(
          rightOp.mkString(", "), leftOp.mkString(", ")),
        "Similarity Scores are equal"
      )
  }

  "Cosine Similarity algorithm" should {

    "return the correct value for OutDir" in {
      val graph = TestGraphs.g6
      val dir = GraphDir.OutDir
      val cosineSimilarity = new CosineSimilarity(graph)

      val similarityScore = cosineSimilarity.calculateSimilarity(dir, 10, 13)
      similarityScore shouldEqual (0.4082482 +- EPSILON)

      val topKSimilarNode = cosineSimilarity.getTopKSimilarNodes(dir, 10, 3)
      topKSimilarNode should almostEqualSeq(Seq((11, 0.408248), (13, 0.408248), (15, 0.408248)))

      val topKAllSimilarNodes = cosineSimilarity.getTopKAllSimilarPairs(dir, 10)
      topKAllSimilarNodes should almostEqualMap(Map(10 -> Seq((11, 0.408248), (13, 0.408248), (15, 0.408248), (12, 0.0), (14, 0.0)),
        11 -> Seq((13, 1.0), (12, 0.707107), (10, 0.408248)),
        12 -> Seq((11, 0.707107), (13, 0.707107)),
        13 -> Seq((11, 1.0), (12,0.707107), (10, 0.408248)),
        14 -> Seq(),
        15 -> Seq((10,0.408248))))
    }

    "return the correct value for InDir" in {
      val graph = TestGraphs.g7_onlyin
      val dir = GraphDir.InDir
      val cosineSimilarity = new CosineSimilarity(graph)

      val similarityScore = cosineSimilarity.calculateSimilarity(dir, 14, 15)
      similarityScore shouldEqual (0.6666667 +- EPSILON)

      val topKSimilarNode = cosineSimilarity.getTopKSimilarNodes(dir, 10, 10)
      topKSimilarNode should almostEqualSeq(Seq((12, 0.408248), (13, 0.408248), (11, 0.333333), (14, 0.333333), (15,0.333333)))

      val topKAllSimilarNodes = cosineSimilarity.getTopKAllSimilarPairs(dir, 10)
      topKAllSimilarNodes should almostEqualMap(Map(10 -> Seq((12, 0.408248), (13, 0.408248), (11, 0.333333), (14, 0.333333), (15, 0.333333), (16, 0.0)),
        11 -> Seq((12, 0.816496), (13, 0.408248), (10, 0.333333), (14, 0.333333), (15, 0.333333)),
        12 -> Seq((11, 0.816497), (13, 0.5), (10, 0.408248)),
        13 -> Seq((12, 0.5), (10, 0.408248), (11, 0.408248)),
        14 -> Seq((15, 0.666667), (16, 0.5773503), (10, 0.3333333), (11, 0.333333)),
        15 -> Seq((14, 0.666667), (10, 0.333333), (11, 0.333333)),
        16 -> Seq((14, 0.577350))))
    }
  }
}
