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

import org.scalatest.WordSpec
import org.scalatest.matchers.ShouldMatchers
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class RedirectsExtractorTest extends WordSpec with ShouldMatchers {
  val resourcePath = "cassovary-wikipedia/src/test/resources/"

  val sampleFileName = "wikipediaSample.xml"

  "Redirects merger" should {
    val rm = new RedirectsMerger
    "inverse map" in {
      val map1 = Map(1 -> 2, 2 -> 3, 3 -> 3, 4 -> 3)
      rm.inverseMap(map1) should be (Map(2 -> Set(1), 3 -> Set(2, 3, 4)))
    }

    "merge set maps" in {
      val map1 = Map(1 -> Set(1, 2), 2 -> Set(2), 3 -> Set(2))
      val map2: Map[Int, Set[Int]] = Map(1 -> Set(2, 3), 2 -> Set(), 4 -> Set(2))

      rm.mergeSetMaps(map1, map2) should be (Map(1 -> Set(1,2,3), 2 -> Set(2), 3 -> Set(2),
        4 -> Set(2)))
    }

    "merge main names maps" in {
      val map1 = Map("Tweeted" -> "Twitter", "United_States" -> "USA")
      val map2 = Map("U.S." -> "USA", "Deoxyribonucleic_acid" -> "DNA")
      val map3 = Map("TWTR" -> "Twitter")

      rm.mergeOtherNames(Seq(map1, map2, map3)) should be (Map("Twitter" -> Set("Tweeted", "TWTR"),
        "USA" -> Set("U.S.", "United_States"), "DNA" -> Set("Deoxyribonucleic_acid")))
    }
  }

  "Redirects extractor" should {
    "extract sample wikipedia dump" in {
      val rm = new RedirectsMerger
      val extractor = new RedirectsExtractor(resourcePath + sampleFileName)
      extractor()
      val redirects = extractor.getRedirects
      val otherNames = rm.mergeOtherNames(Seq(redirects))

      otherNames should be (Map(
        "Computer_accessibility" -> Set("AccessibleComputing"),
        "History_of_Afghanistan" -> Set("AfghanistanHistory"),
        "Geography_of_Afghanistan" -> Set("AfghanistanGeography"),
        "Demography_of_Afghanistan" -> Set("AfghanistanPeople"),
        "Communications_in_Afghanistan" -> Set("AfghanistanCommunications"),
        "Amoeboid" -> Set("AmoeboidTaxa")
        ))
    }
  }
}
