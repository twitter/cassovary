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
package com.twitter.cassovary.wikipedia

import scala.io.Source

class HashArraysMultipleNames(
  otherNames: collection.Map[String, Set[String]],
  mainName: collection.Map[String, String]
) extends MultipleNames[String] {

  def getOtherNames(name: String): Set[String] = otherNames(name)

  def getMainName(name: String): String = mainName(name)
}

object HashArraysMultipleNames {
  val splitChar = "|"
  /**
   * Reads `MultipleNames` from file written in format:
   * {{{
   *   mainName1|otherName11[|otherName1..]*
   *   mainName2|otherName21[|otherName2..]*
   *   ...
   * }}}
   */
  def readFromFile(filename: String) = {
    val otherNames = Source.fromFile(filename).getLines()
      .map { line =>
        val splitted = line.split(splitChar)
        val main = splitted(0)
        val others = splitted.drop(1).toSet
        main -> others
    }.toMap
    val mainName = otherNames.flatMap {case (main, other) => other.map( (_, main))}
    new HashArraysMultipleNames(otherNames, mainName)
  }
}
