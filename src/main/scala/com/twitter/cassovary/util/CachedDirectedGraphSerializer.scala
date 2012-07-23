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

import net.lag.logging.Logger
import java.io._
import collection.mutable
import java.util.concurrent.atomic.AtomicLong
import java.nio.ByteBuffer

/**
 * Convenience class for serializing stuff when loading a CachedDirectedGraph
 * If a directory is not specified, or set to null, a random directory is generated
 * If useCachedValues is set to false, the serializer will always call writeFn and never readFn in writeOrRead
 * @param directory directory to store cached values in
 * @param useCachedValues use the reader if values are available?
 */
class CachedDirectedGraphSerializer(var directory: String, useCachedValues: Boolean) {

  private lazy val log = Logger.get("CachedDirectedGraphSerializer")

  if (directory == null) {
    directory = "temp-cached/" + System.nanoTime()
  }

  new File(directory).mkdirs()

  /**
   * Writer class that allows chaining
   * @param filename filename to write to
   */
  class CachedDirectedGraphSerializerWriter(filename: String) {

    val fos = new FileOutputStream(filename+".temp")
    val fc = fos.getChannel()
    val dos = new DataOutputStream(fos)

    def integers(integers: Iterable[Int]) = {
      integers.foreach(dos.writeInt(_))
      this
    }

    def longs(longs: Iterable[Long]) = {
      longs.foreach(dos.writeLong(_))
      this
    }

    def arrayOfLongInt(ali: Array[(Long, Int)], notNullSize:Int) = {
      println(fc.position())
      dos.writeInt(ali.size)
      dos.writeInt(notNullSize)
      dos.flush()
      println(fc.position())

      var numInsertions = 0
      var remainingSize = notNullSize.toLong * 16
      def makeBB = {
        if (remainingSize - 16000000 >= 0) {
          remainingSize -= 16000000
          ByteBuffer.allocate(16000000)
        }
        else {
          val bb = ByteBuffer.allocate(remainingSize.toInt)
          remainingSize = 0
          bb
        }
      }
      var bb:ByteBuffer = makeBB

      (0 until ali.size).foreach { i =>
        ali(i) match {
          case (j,k) => {
            if (numInsertions == 1000000) {
              bb.rewind()
              fc.write(bb)
              bb = makeBB
              numInsertions = 0
            }
            bb.putInt(i)
            bb.putLong(j)
            bb.putInt(k)
            numInsertions += 1
          }
          case _ => ()
        }
      }
      if (numInsertions > 0) {
        bb.rewind()
        fc.write(bb)
      }

//      val oos = new ObjectOutputStream(fos)
//      oos.writeObject(ali)
//      oos.flush()
      this
    }

    def bitSet(bs: mutable.BitSet) = {
      dos.flush()
      val oos = new ObjectOutputStream(fos)
      oos.writeObject(bs)
      oos.flush()
      this
    }

    def atomicLongArray(ala: Array[AtomicLong]) = {
      dos.flush()
      val oos = new ObjectOutputStream(fos)
      oos.writeObject(ala)
      oos.flush()
      this
    }

    def close = {
      fc.close()
      dos.close()
      new File(filename+".temp").renameTo(new File(filename))
    }
  }

  /**
   * Reader class, no chaining here
   * @param filename filename to read from
   */
  class CachedDirectedGraphSerializerReader(filename: String) {

    val fos = new FileInputStream(filename)
    val bos = new BufferedInputStream(fos)
    val dos = new DataInputStream(bos)

    def int = dos.readInt()

    def long = dos.readLong()

    def integers(length: Int) = {
      (0 until length).map(_ => dos.readInt())
    }

    def longs(length: Int) = {
      (0 until length).map(_ => dos.readLong())
    }

    def arrayOfLongInt() = {
//      val ois = new ObjectInputStream(fos)
//      ois.readObject().asInstanceOf[Array[(Long, Int)]]
      val size = dos.readInt()
      val realSize = dos.readInt()
      val ali = new Array[(Long,Int)](size)
      (0 until realSize).foreach { _ =>
        val idx = dos.readInt()
        val lng = dos.readLong()
        val itg = dos.readInt()
        ali(idx) = (lng, itg)
      }
      ali
    }

    def bitSet(): mutable.BitSet = {
      val ois = new ObjectInputStream(bos)
      ois.readObject().asInstanceOf[mutable.BitSet]
    }

    def atomicLongArray(): Array[AtomicLong] = {
      val ois = new ObjectInputStream(bos)
      ois.readObject().asInstanceOf[Array[AtomicLong]]
    }

    def close = dos.close()
  }

  private def write(filename: String): CachedDirectedGraphSerializerWriter = {
    new CachedDirectedGraphSerializerWriter("%s/%s".format(directory, filename))
  }

  private def read(filename: String): CachedDirectedGraphSerializerReader = {
    new CachedDirectedGraphSerializerReader("%s/%s".format(directory, filename))
  }

  private def exists(filename: String): Boolean = {
    new File("%s/%s".format(directory, filename)).exists
  }

  /**
   * Function that you should call. This decides whether to write to a file or to read from it,
   * depending on whether the given file exists.
   * Provide 2 functions to this function, one for when we need to write values,
   * and one when we want to read values
   * @param filename filename to read/write from/to
   * @param writeFn function that will be called if writing
   * @param readFn function that will be called if reading
   */
  def writeOrRead(filename: String, writeFn: (CachedDirectedGraphSerializerWriter => Unit),
                  readFn: (CachedDirectedGraphSerializerReader => Unit)) = {
    if (!useCachedValues || !exists(filename)) {
      log.info("Recomputing...")
      writeFn(write(filename))
    }
    else {
      log.info("Loading cached version...")
      readFn(read(filename))
    }
  }
}