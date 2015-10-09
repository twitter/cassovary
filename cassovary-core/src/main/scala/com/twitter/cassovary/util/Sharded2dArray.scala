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
package com.twitter.cassovary.util

/**
 * A wrapper that behaves as an Array of internal Arrays of `T`.
 *
 * Internally, however, it keeps only a constant number of arrays, called shards.
 *
 * @param shards shards array
 * @param indicator function that is true iff a given inner array is present
 * @param offsets offsets of each inner array in the shards array
 * @param lengths lengths of each inner array
 * @param hashing function that maps inner array id to corresponding shard
 */
class Sharded2dArray[@specialized(Int, Long) T](shards: Array[Array[T]],
                                                indicator: T => Boolean,
                                                offsets: T => Int,
                                                lengths: T => Int,
                                                hashing: T => Int) {
  def apply(id: T): Seq[T] = {
    if (indicator(id)) {
      new ArraySlice[T](shards(hashing(id)), offsets(id), lengths(id))
    } else {
      null
    }
  }
}
