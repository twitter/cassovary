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
import scala.reflect.ClassTag

object SparseOrArrayBasedInt2ObjectMap {

  def apply[T : ClassTag](isSparse: Boolean, maxId: Option[Int]): mutable.Map[Int, T] = {
    if (isSparse) {
      new Int2ObjectOpenHashMap[T]().asInstanceOf[jutil.Map[Int, T]]
    } else {
      new ArrayBasedInt2ObjectMap[T](maxId.get)
    }
  }

}

/**
 * A set backed by an underlying array that keeps track of ints in the range 0..maxId both
 * inclusive. It can be used concurrently as each set element has its own location in the array.
 * @param maxVal maximum integer value in the set
 */
class ArrayBackedSet(val maxVal: Int) {
  private val underlying = new Array[Byte](maxVal + 1)

  def add(i: Int) = { underlying(i) = 1 }

  def remove(i: Int) = { underlying(i) = 0 }

  def contains(i: Int) = underlying(i) == 1

  def iterator = (0 to maxVal).filter( i => underlying(i) != 0).iterator

}

class ArrayBasedInt2ObjectMap[T : ClassTag](maxId: Int) extends mutable.Map[Int, T] {

  private val array = new Array[T](maxId + 1)
  private val ids = new ArrayBackedSet(maxId)

  def get(id: Int): Option[T] = if (ids.contains(id)) Some(array(id)) else None

  def iterator = ids.iterator.map { id => (id, array(id)) }

  def += (kv: (Int, T)) = {
    array(kv._1) = kv._2
    ids.add(kv._1)
    this
  }

  def -= (id: Int) = {
    ids.remove(id)
    this
  }

}
