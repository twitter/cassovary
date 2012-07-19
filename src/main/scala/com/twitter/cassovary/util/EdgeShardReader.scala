package com.twitter.cassovary.util

import java.io.{RandomAccessFile, DataInputStream, FileInputStream}
import java.nio.ByteBuffer

/**
 * Read binary integers from a file
 * @param filename
 */
class EdgeShardReader(val filename:String) {
  private val rf = new RandomAccessFile(filename, "r") // Alternative - FileOutputStream and DataOutputStream

  /**
   * Read integers from this shard into an array
   * @param offset Byte offset in this shard
   * @param numEdges Number of integers to read out
   * @param intArray Integer array to write integers into
   * @param intArrayOffset Integer offset to write into in the integer array
   */
  def readIntegersFromOffsetIntoArray(offset:Long, numEdges:Int, intArray:Array[Int], intArrayOffset:Int):Unit = {
    rf.seek(offset)
    // Read into byte array, then copy from byte array to given int array
    val bytesToRead = 4 * numEdges
    val byteArray = new Array[Byte](bytesToRead)
    rf.read(byteArray, 0, bytesToRead)
    val ib = ByteBuffer.wrap(byteArray).asIntBuffer()
    ib.get(intArray, intArrayOffset, numEdges)
  }

  def close = rf.close()
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