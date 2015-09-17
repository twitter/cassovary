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

import scala.collection.mutable
import scala.io.Source
import com.twitter.logging.Logger

class MapNodeNumberer[T](externalToInternalMap: collection.Map[T, Int],
                         internalToExternalMap: collection.Map[Int, T])
  extends NodeNumberer[T] {
  override def externalToInternal(externalNodeId: T): Int = externalToInternalMap(externalNodeId)

  override def internalToExternal(internalNodeId: Int): T = internalToExternalMap(internalNodeId)
}

object MapNodeNumberer {

  private lazy val log = Logger.get()

  /**
   * Reads map numberer from file in the following format:
   * {{{
   *   name1 id1
   *   name2 id2
   *   ...
   * }}}
   * for example:
   * {{{
   *   banana 12
   *   apple 2
   *   spoon 8
   * }}}
   */
  def forStringsFromFile(filename: String): MapNodeNumberer[String] = {
    val internalToExternal = mutable.HashMap[Int, String]()
    val externalToInternal = mutable.HashMap[String, Int]()

    Source.fromFile(filename).getLines().foreach {
      line =>
        val lineAsArray = line.split(" ")
        val name = lineAsArray(0)
        val id = ParseString.toInt(lineAsArray(1))
        if (internalToExternal.contains(id))
          throw new Exception("Duplicate id found: " + id)
        internalToExternal += ((id, name))
        if (externalToInternal.contains(name))
          throw new Exception("Duplicate name found: " + name)
        externalToInternal += ((name, id))
    }

    new MapNodeNumberer[String](externalToInternal, internalToExternal)
  }
}