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

import java.io.{IOException, File, DataOutputStream, FileOutputStream}

/**
 * Write integers in binary to a single shard.
 * This class shouldn't normally be used directly, instead use
 * EdgeShardsWriter
 * @param filename
 */
class EdgeShardWriter(val filename:String) {
  private val fos = new FileOutputStream(filename)
  private val fc = fos.getChannel()
  private val dos = new DataOutputStream(fos)
  private var offset = 0

  // TODO might want to preallocate space first to avoid fragmentation

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
    intList.foreach(dos.writeInt(_))
    offset += intList.size * 4
    returnOffset
  }

  /**
   * Write a list of integers at the given byte offset
   * @param writeOffset Byte offset to start writing at
   * @param intList a list of integers
   */
  def writeIntegersAtOffset(writeOffset:Long, intList:Iterable[Int]) = {
    fos.getChannel.position(writeOffset)
    intList.foreach(dos.writeInt(_))
  }

  def close = dos.close()
}

/**
 * Class to write nodes to disk, sharding them according to their id
 * @param shardDirectory
 * @param numShards
 */
class EdgeShardsWriter(val shardDirectory:String, val numShards:Int) {
  { val dir = new File(shardDirectory)
    if (!dir.exists && !dir.mkdirs) throw new IOException("Unable to create EdgeShards directory!") }

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