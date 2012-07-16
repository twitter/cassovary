/*
 * Copyright 2012 Twitter, Inc.
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

import com.twitter.cassovary.graph.{GraphDir, Node}

class ArrayBasedLRUCache[T <: Node](val indexCapacity:Int, val realCapacity:Int, val maxId:Int) {
  var t: T = _ // acts as null, else you can do [T >: Null]

  // Maps
  val idToIndex = new Array[(Int,T)](maxId+1) // to save space you could move T into indexToId
  val indexToId = new Array[Int](indexCapacity+1)

  // Initialize a linked list of free indices
  val freeIndices = new Array[Int](indexCapacity+1)
  (0 until indexCapacity+1).foreach { i => freeIndices(i) = i + 1 }
  freeIndices(indexCapacity) = 0

  // Here's the doubly linked list
  val indexNext = new Array[Int](indexCapacity+1)
  val indexPrev = new Array[Int](indexCapacity+1)
  var head = 0
  var tail = 1

  // Keep track of the capacity of this array!
  var currIndexCapacity, currRealCapacity = 0

  // Add an index to the free index list
  private def addToFree(index:Int) {
    currIndexCapacity -= 1
    freeIndices(index) = freeIndices(0)
    freeIndices(0) = index
  }

  // Pop an index from the free index list
  private def popFromFree():Int = {
    currIndexCapacity += 1
    val popped = freeIndices(0)
    freeIndices(0) = freeIndices(popped)
    popped
  }

  // Remove an item from the cache
  private def removeFromTail() {
    val prevTail = tail
    val prevId = indexToId(prevTail)
    tail = indexNext(prevTail)
    currRealCapacity -= idToIndex(prevId)._2.neighborCount(GraphDir.OutDir)
    addToFree(prevTail)
    idToIndex(prevId) = (0, t)
    indexToId(prevTail) = 0
    indexPrev(tail) = 0
  }

  /**
   * Moves an element to the head of the list
   * For speed, this doesn't check for existence
   */
  def moveToHead(id:Int) {
    if (!contains(id)) throw new IllegalArgumentException("trying to move an item that doesn't exist in the cache!")

    val idx = idToIndex(id)._1
    if (idx != head) {
      val prevIdx = indexPrev(idx)
      val nextIdx = indexNext(idx)
      val prevHeadIdx = head
      if (tail == idx) tail = nextIdx
      // Update pointers
      indexNext(prevIdx) = nextIdx
      indexPrev(nextIdx) = prevIdx
      indexNext(idx) = 0
      indexPrev(idx) = prevHeadIdx
      indexNext(prevHeadIdx) = idx
      head = idx
    }
  }

  /**
   * Adds an element to the list, removing another
   * if there are too many elements
   */
  def addToHead(id:Int, item:T) {
    val eltSize = item.neighborCount(GraphDir.OutDir)
    if (eltSize > realCapacity) throw new IllegalArgumentException("item %s is too large for the cache...".format(id))
    if (contains(id)) throw new IllegalArgumentException("trying to add an item that already exists!")

    val prevHeadIdx = head
    while(currIndexCapacity == indexCapacity || currRealCapacity + eltSize > realCapacity) {
      removeFromTail()
    }
    currRealCapacity += eltSize
    head = popFromFree()
    idToIndex(id) = (head, item)
    indexToId(head) = id
    indexNext(prevHeadIdx) = head
    indexNext(head) = 0
    indexPrev(head) = prevHeadIdx
    if (currIndexCapacity == 1) tail = head
  }

  def contains(id:Int):Boolean = {
    idToIndex(id) != null && idToIndex(id)._1 != 0
  }

  def get(id:Int):T = {
    idToIndex(id)._2
  }
}