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

import java.io._
import java.nio.ByteBuffer

/**
 * Write integers in binary to a single shard.
 * This class shouldn't normally be used directly, instead use
 * EdgeShardsWriter
 * @param filename
 */
class EdgeShardWriter(val filename:String) {
  private val fos = new RandomAccessFile(filename, "rw")
  private var offset = 0

  def intToByteArray(value:Int) = {
    Array[Byte](
      (value >>> 24).toByte,
      (value >>> 16).toByte,
      (value >>> 8).toByte,
      value.toByte)
  }
  def byteArrayToInt(b:Array[Byte]) = {
    (b(0) << 24) | (b(1) << 16) | (b(2) << 8) | b(3)
  }

  /**
   * Read a list of integers to this particular shard and return
   * a byte offset into this particular file for use by EdgeShardReader
   * Will write from wherever the write position is at
   * @param intList the list of integers to write to this shard
   */
  def writeIntegersSequentially(intList:Iterable[Int]):Long = {
    val returnOffset = offset // can use fc.position() but you must call dos.flush() first
    intList.foreach(fos.writeInt(_))
    offset += intList.size * 4
    returnOffset
  }

  /**
   * Write a list of integers at the given byte offset
   * @param writeOffset Byte offset to start writing at
   * @param intList a list of integers
   */
  def writeIntegersAtOffset(writeOffset:Long, intList:Iterable[Int]) = {
    fos.seek(writeOffset)
    intList.foreach(fos.writeInt(_))
  }

  def allocate(fileSize:Long) = {
    if (fileSize > 0) {
      fos.seek(fileSize-1)
      fos.writeByte(1)
    }
  }

  def length = fos.length

  def close = fos.close()
}

/**
 * Class to write nodes to disk, sharding them according to their id
 * A node with the id N is written to shard (N mod numShards).txt
 * @param shardDirectory Directory to write shards to
 * @param numShards Number of shards to generate
 */
class EdgeShardsWriter(val shardDirectory:String, val numShards:Int) {
  new File(shardDirectory).mkdirs()
//  { val dir = new File(shardDirectory)
//    if (!dir.exists && !dir.mkdirs) throw new IOException("Unable to create EdgeShards directory %s".format(shardDirectory)) }

  val shardWriters = (0 until numShards).map { i =>
    new EdgeShardWriter("%s/%s.txt".format(shardDirectory, i))
  }

  def writeIntegersSequentially(nodeId:Int, intList:Iterable[Int]):Long = {
    shardWriters(nodeId % numShards).writeIntegersSequentially(intList)
  }

  def writeIntegersAtOffset(nodeId:Int, writeOffset:Long, intList:Iterable[Int]) = {
    shardWriters(nodeId % numShards).writeIntegersAtOffset(writeOffset, intList)
  }

  def close = (0 until numShards).foreach { i => shardWriters(i).close }
}

/**
 * Write out shards to different directories
 * Shard N is written to the directory at index (N mod NUMBER_OF_DIRECTORIES)
 * @param shardDirectories Array of directory strings
 * @param numShards Number of shards to generate
 */
class MultiDirEdgeShardsWriter(val shardDirectories:Array[String], numShards:Int)
  extends EdgeShardsWriter(shardDirectories(0), numShards) {

  shardDirectories.foreach { shardDirectory => new File(shardDirectory).mkdirs() }

  override val shardWriters = (0 until numShards).map { i =>
    new EdgeShardWriter("%s/%s.txt".format(shardDirectories(i % shardDirectories.length), i))
  }
}

/**
 * Used by MemEdgeShardsWriter
 * This is an in-memory shard, which can be written to disk if so desired.
 * @param shardSize
 */
class MemEdgeShardWriter(val shardSize:Int) {
  val shard = new Array[Int](shardSize)

  def writeIntegersAtOffsetFromOffset(writeIntOffset:Int, sourceArray:Array[Int], sourceIntOffset:Int, sourceLength:Int) = {
    Array.copy(sourceArray, sourceIntOffset, shard, writeIntOffset, sourceLength)
  }

  def writeToShard(filename:String) {
    // Should be faster than DataOutputStream
    val bb = ByteBuffer.allocateDirect(4 * shard.size)
    shard.foreach(bb.putInt(_))
    bb.rewind()
    val fc = new FileOutputStream(filename).getChannel()
    fc.write(bb)
    fc.close()
  }
}

/**
 * Convenience class to write shards to disk in rounds
 * How to use:
 * 1) For each round, call startRound to initialize the in-memory shards.
 * 2) Using writeIntegersAtOffsetFromOffset, write to these in-memory shards the nodes of appropriate ids
 *    - Only ids which when mod numShards, are in roundRange = [modStart, modEnd) are written to an in-memory shard
 *    - Ex. if the current roundRange is [0, 2) and numShards is 6, ids 1 & 6 are written but id 3 is ignored.
 * 3) Call endRound to write the shards to disk
 * Do not create multiple copies of this class that write to the same directory - behavior is undefined.
 * @param shardDirectory shard directory
 * @param numShards the number of shards
 * @param shardSizes the size of each shard
 * @param rounds number of write rounds
 */
class MemEdgeShardsWriter(val shardDirectory:String, val numShards:Int, val shardSizes:Array[Int], val rounds:Int=1) {
  new File(shardDirectory).mkdirs()
//  { val dir = new File(shardDirectory)
//    if (!dir.exists && !dir.mkdirs) throw new IOException("Unable to create EdgeShards directory %s".format(shardDirectory)) }

  var roundNo = 0
  val writers = new Array[MemEdgeShardWriter](numShards)
  var modStart = 0
  var modEnd = 0

  assert(rounds <= numShards)

  /**
   * Initialize shards in this round and initialize in-memory shards
   * @param roundNo the round number
   */
  def startRound(roundNo:Int) = {
    modStart = (roundNo.toDouble / rounds * numShards).toInt
    modEnd = ((roundNo+1).toDouble / rounds * numShards).toInt
    (modStart until modEnd).foreach { i =>
      writers(i) = new MemEdgeShardWriter(shardSizes(i))
    }
  }

  /**
   * Write shards to disk sequentially
   * Always call this after starting a round
   */
  def endRound = { // Write shards to disk
    (modStart until modEnd).foreach { i =>
      writers(i).writeToShard("%s/%s.txt".format(shardDirectory, i))
      writers(i) = null
    }
  }

  def roundRange = (modStart, modEnd)

  /**
   * Write integers into an array in memory.
   * Note that unlike EdgeShardsWriter, the write offset is also an int offset, since we
   * write into an int array
   * @param nodeId Id of edges to write
   * @param writeIntOffset Integer offset to write to
   * @param sourceArray Source array to read from
   * @param sourceIntOffset Offset in source array to read from
   * @param sourceLength Number of integers to read from the source array
   */
  def writeIntegersAtOffsetFromOffset(nodeId:Int, writeIntOffset:Int, sourceArray:Array[Int], sourceIntOffset:Int, sourceLength:Int) = {
    val shardNum = nodeId % numShards
    if (modStart <= shardNum && shardNum < modEnd) {
      writers(shardNum).writeIntegersAtOffsetFromOffset(writeIntOffset, sourceArray, sourceIntOffset, sourceLength)
    }
    else {
      throw new IllegalArgumentException("Invalid nodeId attempted to be written")
    }
  }

}

/**
 * Version of MemEdgeShardsWriter that writes to multiple directories
 * @param shardDirectories an array of directory strings
 * @param numShards the number of shards
 * @param shardSizes the size of each shard
 * @param rounds number of write rounds
 */
class MultiDirMemEdgeShardsWriter(val shardDirectories:Array[String], numShards:Int, shardSizes:Array[Int], rounds:Int=1)
  extends MemEdgeShardsWriter(shardDirectories(0), numShards, shardSizes, rounds) {

  shardDirectories.foreach { shardDirectory => new File(shardDirectory).mkdirs() }

  override def endRound = {
    (modStart until modEnd).foreach { i =>
      writers(i).writeToShard("%s/%s.txt".format(shardDirectories(i % shardDirectories.length), i))
      writers(i) = null
    }
  }

}