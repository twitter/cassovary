package com.twitter.cassovary.util

import scala.collection.mutable
import scala.reflect.ClassTag

/**
 * An Int -> Object map, where underlying ints must be in the range of 0..maxId
 * and are stored in an array.
 * This map is NOT space efficient for sparse maps, but offers two advantages:
 * (1) Fast, as indexing is simple
 * (2) Updates and reads can be used concurrently, since there is no resizing operation.
 */

class ArrayBackedInt2ObjectMap[T : ClassTag](maxId: Int) extends mutable.Map[Int, T] {

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
