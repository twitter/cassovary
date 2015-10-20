package com.twitter.cassovary.util.collections

import com.twitter.cassovary.util.ArrayBackedSet

import scala.collection.mutable

class ArrayBackedInt2IntMap(val maxId: Int) extends mutable.Map[Int, Int] {
  private val array = new Array[Int](maxId + 1)
  private val ids = new ArrayBackedSet(maxId)

  def get(id: Int): Option[Int] = if (ids.contains(id)) Some(array(id)) else None

  def iterator = ids.iterator.map { id => (id, array(id)) }

  override def keysIterator: Iterator[Int] = ids.iterator

  def += (kv: (Int, Int)) = {
    array(kv._1) = kv._2
    ids.add(kv._1)
    this
  }

  def -= (id: Int) = {
    ids.remove(id)
    this
  }

}
