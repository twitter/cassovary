package com.twitter.cassovary.util

import java.io.{DataInputStream, FileInputStream}
import java.nio.ByteBuffer

/**
 * Read binary integers from a file
 * @param filename
 */
class EdgeShardReader(val filename:String) {
  private val fis = new FileInputStream(filename)
  private val dis = new DataInputStream(fis)

  /**
   * Read integers from this shard into an array
   * @param offset Byte offset in this shard
   * @param numEdges Number of integers to read out
   * @param intArray Integer array to write integers into
   * @param intArrayOffset Offset to write into in the integer array
   */
  def readIntegersFromOffsetIntoArray(offset:Long, numEdges:Int, intArray:Array[Int], intArrayOffset:Int):Unit = {
    fis.getChannel.position(offset)
    // Read into byte array, then copy from byte array to given int array
    val bytesToRead = 4 * numEdges
    val byteArray = new Array[Byte](bytesToRead)
    dis.read(byteArray, 0, bytesToRead)
    val ib = ByteBuffer.wrap(byteArray).asIntBuffer()
    ib.get(intArray, intArrayOffset, numEdges)
  }

  def close = dis.close()
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

  def readIntegersFromOffsetIntoArray(nodeId:Int, offset:Long, numEdges:Int, intArray:Array[Int], intArrayOffset:Int):Unit = {
    shardReaders(nodeId % numShards).readIntegersFromOffsetIntoArray(offset, numEdges, intArray, intArrayOffset)
  }

  def close = (0 until numShards).foreach { i => shardReaders(i).close }
}