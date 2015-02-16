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

import scala.collection.mutable
import scala.reflect.runtime.universe.TypeTag

/**
 * A trait to support adding labels to nodes and edges.
 * A label is of any type, has a name and optionally maps an 'id' (such as node-id or edge-id)
 * to a value. Labels need not be defined for all ids.
 *
 * @tparam ID the id-type of the node or edges (e.g., Int)
 * @tparam L  the type of the label value
 */
trait Label[ID, L] extends mutable.Map[ID, L] {
  val name: String
  val tag: TypeTag[L]
}

// the simplest of labels backed by a mutable Hashmap
class MapBasedLabel[ID, L](val name: String)(implicit val tag: TypeTag[L])
  extends mutable.HashMap[ID, L] with Label[ID, L]

// A way to implement a partially-defined label is to keep ids for which the
// labels are defined in a set.
trait PartialLabel[ID, L] extends Label[ID, L] {
  def underlying: mutable.Set[ID]
  private def isThere(id: ID): Boolean = underlying.contains(id)

  abstract override def get(id: ID) = {
    if (isThere(id)) super.get(id) else None
  }

  abstract override def iterator = {
    val i = super.iterator
    i filter { case (id, v) => isThere(id) }
  }

  abstract override def +=(kv: (ID, L)) = {
    underlying += kv._1
    super.+=(kv)
  }

  abstract override def -=(id: ID) = {
    underlying -= id
    // note we do not call super's -= method
    this
  }
}

// A Boolean label that always returns true. To be mixed in with PartialLabel to create
// a flag label.
abstract class TrueLabel[ID] extends Label[ID, Boolean] { self: PartialLabel[ID, Boolean] =>
  def get(id: ID): Option[Boolean] = Some(true)
  def iterator = underlying.iterator.map { case x => (x, true) }
  def +=(kv: (ID, Boolean)) = this
  def -=(id: ID) = this
}

/**
 * This label is suitable to keep a 'flag' type label which some ids have and others don't.
 * Those ids that have it return `Some(x)` and those don't have return `None`. `x` does not
 * matter and here we use `true.`
 */
class FlagLabel[ID](val name: String, val underlying: mutable.Set[ID])(implicit val tag: TypeTag[Boolean])
  extends TrueLabel[ID] with PartialLabel[ID, Boolean]