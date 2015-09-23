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
import scala.reflect.ClassTag
import scala.reflect.runtime.universe.TypeTag

trait HasMaxId {
  val maxId: Int
}

/**
 * A subtype of Label that uses an underlying array to map an integer id to the value
 * associated with that id. Useful when `maxId` is not too big.
 * @see An array based label must be defined for all ids. If desired for only some ids,
 *      mix it in with `PartialLabel`
 * @param name  name of the label
 * @param maxId maximum value of the id
 * @tparam L    type of the label value
 *     ClassTag is required to be able to build an array of type L
 *    TypeTag is required to be able to retrieve values of type L from the Label
 */
class ArrayBasedLabel[L : ClassTag: TypeTag](val name: String,
    val maxId: Int) extends Label[Int, L] with HasMaxId {
  private val array = new Array[L](maxId + 1)
  val tag = implicitly[TypeTag[L]]

  def get(id: Int): Option[L] = Some(array(id))

  def iterator = array.iterator.zipWithIndex map { case (v, id) => (id, v) }

  def += (kv: (Int, L)) = {
    array(kv._1) = kv._2
    this
  }

  def -= (id: Int) = {
    throw new UnsupportedOperationException("Removal of an array based label is not allowed")
    this
  }
}

/**
 * A partially defined label containing values of type L. The tracking of which ids
 * have the label defined is done by a BitSet, hence this trait must have self type HasMaxId.
 */
trait BitSetBasedPartialLabel[L] extends PartialLabel[Int, L] {
  self: HasMaxId =>
  val underlying = new mutable.BitSet(maxId + 1)
}

/**
 * Backed by an array, but allows some ids to have undefined ('None') value (the ids present
 * are tracked by a bitset).
 */
class ArrayBasedPartialLabel[L : ClassTag: TypeTag]
(override val name: String, override val maxId: Int)
    extends ArrayBasedLabel[L](name, maxId) with BitSetBasedPartialLabel[L]

/**
 * A partially defined flag label backed by a bitset.
 */
class BitSetBasedFlagLabel(val name: String, val maxId: Int)(implicit val tag: TypeTag[Boolean])
    extends TrueLabel[Int] with BitSetBasedPartialLabel[Boolean] with HasMaxId
