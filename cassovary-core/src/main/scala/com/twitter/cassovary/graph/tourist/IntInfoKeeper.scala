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

package com.twitter.cassovary.graph.tourist

import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap
import java.util
import scala.collection.JavaConversions._
import scala.collection.mutable

/**
 * An InfoKeeper keeps caller-supplied info per node.
 * It provides another method recordInfo() to record a given info for a node.
 */
class IntInfoKeeper(override val onlyOnce: Boolean) extends InfoKeeper[Int] {
  protected val underlyingMap =  new Int2IntOpenHashMap

  override protected def infoPerNode: mutable.Map[Int, Int] = underlyingMap.asInstanceOf[util.Map[Int, Int]]
}
