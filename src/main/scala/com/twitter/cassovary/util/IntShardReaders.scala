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

import com.twitter.ostrich.stats.Stats
import java.io.RandomAccessFile
import java.nio.ByteBuffer

/**
 * Read consecutively written binary integers from a file
 * @param filename
 */
class SingleIntShardReader(val filename:String) {
  private val rf = new RandomAccessFile(filename, "r") // Alternative - FileOutputStream and DataOutputStream

  /**
   * Read a specified number of integers from this shard, starting
   * at some integer offset, into an an array, starting at some other offset.
   * Behavior is undefined if attempting to read past the end of the file.
   *
   * @param intOffset int offset in this shard
   * @param numInts Number of integers to read out
   * @param intArray Integer array to write integers into
   * @param intArrayOffset Integer offset to write into in the integer array
   */
  def readIntegersFromOffsetIntoArray(intOffset: Long, numInts: Int, intArray: Array[Int],
      intArrayOffset: Int) = Stats.time("esr_fullread") {
    rf.seek(intOffset * 4)
    // Read into byte array, then copy from byte array to given int array
    val bytesToRead = 4 * numInts
    val byteArray = new Array[Byte](bytesToRead)
    rf.read(byteArray, 0, bytesToRead)
    val ib = ByteBuffer.wrap(byteArray).asIntBuffer()
    ib.get(intArray, intArrayOffset, numInts)
  }

  def close = {
    rf.close()
  }
}

/**
 * Read binary integers from shards
 * Shards must be in the form N.txt, where N is from 0 to numShards-1
 * @param shardDirectory
 * @param numShards
 */
class IntShardsReader(val shardDirectory: String, val numShards: Int) {
  val shardReaders = (0 until numShards).map { i =>
    new SingleIntShardReader("%s/%s.txt".format(shardDirectory, i))
  }

  /**
   * Read some number of integers from the given integer offset into a given array
   * starting at some offset in that array. Does not check file or array bounds.
   *
   * @param nodeId id of node desired
   * @param intOffset int offset in the file
   * @param numInts number of integers to read out
   * @param intArray destination integer array to read into
   * @param intArrayOffset offset of integer array to read to
   */
  def readIntegersFromOffsetIntoArray(nodeId: Int, intOffset: Long, numInts: Int,
                                      intArray: Array[Int], intArrayOffset: Int):Unit = {
    shardReaders(nodeId % numShards).readIntegersFromOffsetIntoArray(intOffset, numInts, intArray, intArrayOffset)
  }

  def close = (0 until numShards).foreach { i => shardReaders(i).close }
}

/**
 * Read binary integers from shards located in multiple directories
 * Shards must be in the form N.txt, where N is from 0 to numShards-1
 * Shard N is accessed from the directory at index (N mod NUMBER_OF_DIRECTORIES)
 * I.e., for some shard N.txt, N must be located in the directory at index N mod NUMBER_OF_DIRECTORIES
 * For example, if 4 directories are provided, shard 6.txt must be located in the third directory
 * An easy way if you don't really care about disk space is to simply replicate the whole original shard directory
 * to all desired directories.
 *
 * @param shardDirectories Array of shard directories
 * @param numShards Total number of shards
 */
class MultiDirIntShardsReader(val shardDirectories: Array[String], val numShards: Int) {
  val shardReaders = (0 until numShards).map { i =>
    new SingleIntShardReader("%s/%s.txt".format(shardDirectories(i % shardDirectories.length), i))
  }

  def readIntegersFromOffsetIntoArray(nodeId:Int, intOffset:Long, numInts:Int, intArray:Array[Int], intArrayOffset:Int):Unit = {
    shardReaders(nodeId % numShards).readIntegersFromOffsetIntoArray(intOffset, numInts, intArray, intArrayOffset)
  }

  def close = (0 until numShards).foreach { i => shardReaders(i).close }
}