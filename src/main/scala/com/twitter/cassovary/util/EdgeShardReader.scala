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

import java.io.{RandomAccessFile, DataInputStream, FileInputStream}
import java.nio.ByteBuffer
import com.twitter.ostrich.stats.Stats
import collection.mutable

/**
 * Read binary integers from a file
 * @param filename
 */
class EdgeShardReader(val filename:String) {
  // Test existence of the file
  new RandomAccessFile(filename, "r").close() // Alternative - FileOutputStream and DataOutputStream

  private val rfs = new mutable.HashMap[Long, RandomAccessFile]

  /**
   * Read integers from this shard into an array
   * @param offset Byte offset in this shard
   * @param numEdges Number of integers to read out
   * @param intArray Integer array to write integers into
   * @param intArrayOffset Integer offset to write into in the integer array
   */
  def readIntegersFromOffsetIntoArray(offset:Long, numEdges:Int, intArray:Array[Int],
      intArrayOffset:Int):Unit = Stats.time("esr_fullread") {

    val rf: RandomAccessFile = try {
      rfs(Thread.currentThread().getId)
    } catch {
      case e: NoSuchElementException => {
        val raf = new RandomAccessFile(filename, "r")
        rfs(Thread.currentThread().getId) = raf
        raf
      }
    }

    rf.seek(offset)
    // Read into byte array, then copy from byte array to given int array
    val bytesToRead = 4 * numEdges
    val byteArray = new Array[Byte](bytesToRead)
    rf.read(byteArray, 0, bytesToRead)
    val ib = ByteBuffer.wrap(byteArray).asIntBuffer()
    ib.get(intArray, intArrayOffset, numEdges)
  }

  def close = {
    rfs.foreach { case (k, v) => v.close() }
  }
}

/**
 * Read binary integers from shards
 * @param shardDirectory
 * @param numShards
 */
class EdgeShardsReader(val shardDirectory:String, val numShards:Int) {
  val shardReaders = (0 until numShards).map { i =>
    new EdgeShardReader("%s/%s.txt".format(shardDirectory, i))
  }

  /**
   * Read Integers from the given offset into a given array
   * Note that the offset into the file is a byte offset,
   * but the offset into the int array is an integer offset
   * @param nodeId id of node desired
   * @param offset byte offset in the file
   * @param numEdges number of integers to read out
   * @param intArray destination integer array to read into
   * @param intArrayOffset offset of integer array to read to
   */
  def readIntegersFromOffsetIntoArray(nodeId:Int, offset:Long, numEdges:Int, intArray:Array[Int], intArrayOffset:Int):Unit = {
    shardReaders(nodeId % numShards).readIntegersFromOffsetIntoArray(offset, numEdges, intArray, intArrayOffset)
  }

  def close = (0 until numShards).foreach { i => shardReaders(i).close }
}

class MultiDirEdgeShardsReader(val shardDirectories: Array[String], val numShards: Int) {
  val shardReaders = (0 until numShards).map { i =>
    new EdgeShardReader("%s/%s.txt".format(shardDirectories(i % shardDirectories.length), i))
  }

  def readIntegersFromOffsetIntoArray(nodeId:Int, offset:Long, numEdges:Int, intArray:Array[Int], intArrayOffset:Int):Unit = {
    shardReaders(nodeId % numShards).readIntegersFromOffsetIntoArray(offset, numEdges, intArray, intArrayOffset)
  }

  def close = (0 until numShards).foreach { i => shardReaders(i).close }
}