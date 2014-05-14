/*
 * Copyright 2014 Twitter, Inc.
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

import com.google.common.primitives.UnsignedBytes
import java.io.{FileOutputStream, RandomAccessFile}
import java.nio.channels.FileChannel
import java.nio.channels.FileChannel.{MapMode => Mode}
import java.nio.file.{Files, Paths}
import java.nio.{ByteBuffer, MappedByteBuffer}
import java.util.Arrays
import scala.collection.mutable.ListBuffer

/**
 * Constant database implementation inspired by http://cr.yp.to/cdb.html.
 * Unlike cdb, there is no limit on the total database size.  A single entry
 * must be smaller than 2GB. Additionally, when creating the database the input
 * keys must be provided in sorted order
 *
 * The database is stored under a dedicated folder. It consists of one
 * index.bin file, and multiple data files. The ith data file is named
 * part<i>.bin
 *
 * JVM does not allow you to memory map a file greater than 2GB. This is set as
 * the max part file size by default
 *
 * The index file has the following format.
 * -----------------------------------------------------------------
 * | part0 key size | part0 key | ... | partn key size | partn key |
 * -----------------------------------------------------------------
 * 
 * The key stored in the index file is the least key in that part. To lookup
 * the value of a key, first the part is selected by consulting the index, then
 * we lookup the key in the part
 *
 * The part files are stored as described in http://cr.yp.to/cdb/cdb.txt
 */
class ConstantDB[K, V](
  dir: String,
  keySerializer: K => ByteBuffer,
  keyDeserializer: ByteBuffer => K,
  valueDeserializer: ByteBuffer => V
)(
  implicit ord: Ordering[K]
) {
  import ConstantDB._

  def apply(key: K): Option[V] = {
    lookup(keySerializer(key)) map { valueDeserializer(_) }
  }

  private val index: IndexedSeq[K] = {
    val path = Paths.get("%s/index.bin".format(dir))
    val bb = ByteBuffer.wrap(Files.readAllBytes(path))
    var partIdx: ListBuffer[K] = ListBuffer.empty
    while (bb.position() < bb.limit()) {
      if (bb.limit() - bb.position() < 4) throw new Exception("Invalid index file")
      val sz = bb.getInt()
      val keybb = bb.duplicate()
      keybb.position(bb.position())
      keybb.limit(bb.position() + sz)
      bb.position(bb.position() + sz)
      partIdx.append(keyDeserializer(keybb))
    }
    partIdx.toIndexedSeq
  }

  val numParts = index.size

  private val bufs: Array[MappedByteBuffer] = {
    (0 until numParts).toArray map { i =>
      val file = new RandomAccessFile("%s/part%s.bin".format(dir, i), "r")
      file.getChannel().map(Mode.READ_ONLY, 0, file.length())
    }
  }


  private def getBuf(input: ByteBuffer): Option[MappedByteBuffer] = {
    val k = keyDeserializer(input.duplicate())
    val idx = index.lastIndexWhere(currK => ord.lteq(currK, k))
    if (idx == -1) {
      Some(bufs.last)
    } else {
      Some(bufs(idx))
    }
  }

  private def lookup(input: ByteBuffer): Option[ByteBuffer] = {
    getBuf(input.duplicate()) flatMap { bb =>
      val h = hash(input.duplicate())
      val hashPtrIdx = (h % 256) * (4+4)
      val hashOffset = bb.getInt(hashPtrIdx)
      val numSlots = bb.getInt(hashPtrIdx + 4)

      if (numSlots == 0) {
        None
      } else {
        val start = (h >> 8) % numSlots
        (0 until numSlots).view
          .map { i => 
            val rotatedIdx = (i + start) % numSlots
            val currHashPtr = hashOffset + (rotatedIdx * 8)
            (bb.getInt(currHashPtr), bb.getInt(currHashPtr + 4)) match {
              case (h1, p1) if p1 == 0 || h1 == 0 =>
                (None, false)
              case (h1, p1) if h1 == h =>
                val keyLength = bb.getInt(p1)
                val valLength = bb.getInt(p1+4)
                input.mark()
                if (keyLength != input.remaining() || !((0 until keyLength) forall { i => bb.get(p1+8+i) == input.get() })) {
                  input.reset()
                  (None, true)
                } else {
                  val sliced = bb.slice()
                  val valStart = p1+8+keyLength
                  sliced.position(valStart)
                  sliced.limit(valStart + valLength)
                  (Some(sliced), false)
                }
              case _ => (None, true)
            }
          }
          .collectFirst { case (res, false) => res }
          .getOrElse(None)
      }
    }
  }
}

object ConstantDB {
  private val headerSize = (4+4)*256

  private def intToByteBuffer(i: Int) = {
    val bb = ByteBuffer.allocate(4)
    bb.putInt(i)
    bb.flip()
    bb
  }

  private def hash(key: ByteBuffer, initial: Long = 5381): Int = {
    var h = initial
    while (key.position() < key.limit()) {
      h = (((h << 5) + h) ^ key.get()) & 0xffffffffL
    }
    math.abs(h.toInt)
  }

  private def getFileChannel(
    folder: String,
    part: Int
  ) = {
    (new FileOutputStream("%s/part%s.bin".format(folder, part))).getChannel()
  }


  def write(
    folder: String,
    data: Stream[(ByteBuffer, ByteBuffer)],
    maxSize: Int = Int.MaxValue
  ) {
    if (data.nonEmpty) {
      val (key, _) = data.head
      val part = 0
      doWrite(folder, part, getFileChannel(folder, part), headerSize, headerSize, Map.empty, data, Seq(key), None, maxSize, 0)
    }
  }

  private def doWrite(
    folder: String,
    part: Int,
    fileChannel: FileChannel,
    sizeSoFar: Int,
    pos: Int,
    buckets: Map[Int, Seq[(Int, Int)]],
    data: Stream[(ByteBuffer, ByteBuffer)],
    partIndex: Seq[ByteBuffer],
    last: Option[ByteBuffer],
    maxSize: Int = Int.MaxValue,
    numWritten: Int = 0
  ) {
    def writeHashesAndHeader {
      (0 until 256) map { id =>
        val hashes = buckets.getOrElse(id, Nil)
        val ncells = hashes.size * 2
        val cells = (0 until ncells) .map { _ => (0, 0) }.toArray
        hashes foreach { case (h, p) =>
          var idx = (h >> 8) % ncells
          while (cells(idx)._1 != 0) {
            idx = (idx + 1) % ncells
          }
          cells(idx) = (h, p)
        }
        cells foreach { case (h, p) =>
          fileChannel.write(intToByteBuffer(h))
          fileChannel.write(intToByteBuffer(p))
        }
      }
      fileChannel.position(0)
      var posHash = pos
      (0 until 256) map { id =>
        val hashes = buckets.getOrElse(id, Nil)
        fileChannel.write(intToByteBuffer(posHash))
        fileChannel.write(intToByteBuffer(hashes.size * 2))
        posHash += (hashes.size * 2) * (4 + 4)
      }
      fileChannel.close()
    }

    data match {
      case Stream.Empty => {
        writeHashesAndHeader
        val fos = new FileOutputStream("%s/index.bin".format(folder)).getChannel()
        try {
          partIndex foreach { key: ByteBuffer =>
            fos.write(intToByteBuffer(key.remaining()))
            fos.write(key)
          }
        } finally {
          fos.close()
        }
      }

      case (key, value) #:: rest => {
        val incr = key.remaining() + value.remaining() + 24
        val newSize = sizeSoFar + incr
        if (newSize <= maxSize) {
          fileChannel.position(pos)
          fileChannel.write(intToByteBuffer(key.remaining()))
          fileChannel.write(intToByteBuffer(value.remaining()))
          fileChannel.write(key.duplicate())
          fileChannel.write(value)
          val h = hash(key.duplicate())
          val hashBucket = h % 256
          val newBuckets = buckets + (hashBucket -> (buckets.getOrElse(hashBucket, Nil) :+ (h, pos)))
          doWrite(folder, part, fileChannel, newSize, fileChannel.position().toInt, newBuckets, rest, partIndex, Some(key), maxSize, numWritten + 1)
        } else {
          writeHashesAndHeader
          if (numWritten == 0) throw new Exception("A single record is larger than allowed max size")
          doWrite(folder, part+1, getFileChannel(folder, part+1), headerSize, headerSize, Map.empty, data, partIndex :+ key, None, maxSize, 0)
        }
      }
    }
  }
}
