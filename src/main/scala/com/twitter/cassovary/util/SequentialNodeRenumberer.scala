/*
 * Copyright 2013 Twitter, Inc.
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

import scala.collection.mutable.{ArrayBuffer,HashMap}

/*
 * Renumbers node id values read from source file to internal sequentially increasing id value.
   Uses a synchronized map for thread safety when shared by multiple reader threads.
 */
class SequentialNodeRenumberer extends NodeRenumberer {

  val externalToInternalMap = HashMap[Int,Int]()
  var internalToExternalMap = ArrayBuffer[Int]()

  def externalToInternal(externalNodeId: Int): Int =
    this.synchronized {
      val internalNodeId = externalToInternalMap.getOrElseUpdate(externalNodeId, externalToInternalMap.size)

      // Note: Assertions are O(1) consistency checks which can be commented out for performance.
      // When loaded correctly we should not be seeing internal ids beyond length of externalToInternalMap.
      assert(externalToInternalMap.size > internalNodeId)
      // InternalNodeId should be no larger than internalToExternalMap.
      assert(internalToExternalMap.size >= internalNodeId)

      if (internalToExternalMap.size == internalNodeId) {
        internalToExternalMap += externalNodeId
      }

      // The internal id should be assigned to the correct external id.
      assert(internalToExternalMap(internalNodeId) == externalNodeId)

      internalNodeId
    }

  def internalToExternal(internalNodeId: Int): Int = {
    internalToExternalMap(internalNodeId)
  }

}
