package com.twitter.cassovary.util

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

import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap
import java.{util => jutil}
import scala.collection.JavaConversions._
import scala.collection.mutable

object SparseOrArrayBasedInt2ObjectMap {

  def apply[T](isSparse: Boolean, maxId: Option[Int]): mutable.Map[Int, T] = {
    if (isSparse) {
      new Int2ObjectOpenHashMap[T]().asInstanceOf[jutil.Map[Int, T]]
    } else {
      new ArrayBasedInt2ObjectMap[T](maxId.get)
    }
  }

}

class ArrayBasedInt2ObjectMap[T](maxId: Int) extends mutable.Map[Int, T] {

  private val array = new Array[T](maxId + 1)
  private val ids = new mutable.BitSet(maxId + 1)

  def get(id: Int): Option[T] = if (ids.contains(id)) Some(array(id)) else None

  def iterator = ids.iterator.map { id => (id, array(id)) }

  def += (kv: (Int, T)) = {
    array(kv._1) = kv._2
    ids += kv._1
    this
  }

  def -= (id: Int) = {
    ids -= kv._1
    this
  }

}
