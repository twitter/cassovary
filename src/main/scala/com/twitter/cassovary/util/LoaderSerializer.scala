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
 * Convenience class for caching stuff on disk when loading anything that takes a long time to load
 * Example - storing maxNodeId so that you don't need to loop through the whole graph to find it
 * If a directory is not specified, or set to null, a random directory is generated
 * If useCachedValues is set to false, the serializer will always call writeFn and never readFn in writeOrRead
 * @param directory directory to store cached values in
 * @param useCachedValues use the reader if values are available?
 */
class LoaderSerializer(var directory: String, useCachedValues: Boolean) {

  private lazy val log = Logger.get("LoaderSerializer")

  if (directory == null) {
    directory = "temp-cached/" + System.nanoTime()
  }

  new File(directory).mkdirs()

  /**
   * Writer class that allows chaining
   * @param filename filename to write to
   */
  class LoaderSerializerWriter(filename: String) {

    val fos = new FileOutputStream(filename+".temp")
    val fc = fos.getChannel()
    val dos = new DataOutputStream(fos)

    def int(i: Int) = {
      dos.writeInt(i)
      this
    }

    def long(l: Long) = {
      dos.writeLong(l)
      this
    }

    def integers(integers: Iterable[Int]) = {
      integers.foreach(dos.writeInt(_))
      this
    }

    def longs(longs: Iterable[Long]) = {
      longs.foreach(dos.writeLong(_))
      this
    }

    def arrayOfLong(al: Array[Long]) = {
      dos.writeInt(al.size)
      dos.flush()

      var bb:ByteBuffer = ByteBuffer.allocate(1000000)
      al.foreach { i =>
        if (bb.remaining() < 8) {
          bb.flip()
          fc.write(bb)
          bb.clear()
        }
        bb.putLong(i)
      }

      if (bb.position() > 0) {
        bb.flip()
        fc.write(bb)
      }
      this
    }

    def arrayOfInt(al: Array[Int]) = {
      dos.writeInt(al.size)
      dos.flush()

      var bb:ByteBuffer = ByteBuffer.allocate(1000000)
      al.foreach { i =>
        if (bb.remaining() < 4) {
          bb.flip()
          fc.write(bb)
          bb.clear()
        }
        bb.putInt(i)
      }

      if (bb.position() > 0) {
        bb.flip()
        fc.write(bb)
      }

//      var numInsertions = 0
//      var remainingSize: Long = al.size * 4
//      def makeBB = {
//        if (remainingSize - 4000000 >= 0) {
//          remainingSize -= 4000000
//          ByteBuffer.allocate(4000000)
//        }
//        else {
//          val bb = ByteBuffer.allocate(remainingSize.toInt)
//          remainingSize = 0
//          bb
//        }
//      }
//      var bb:ByteBuffer = makeBB
//      al.foreach { i =>
//        if (numInsertions == 1000000) {
//          bb.rewind()
//          fc.write(bb)
//          bb = makeBB
//          numInsertions = 0
//        }
//        bb.putInt(i)
//        numInsertions += 1
//      }
//
//      if (numInsertions > 0) {
//        bb.rewind()
//        fc.write(bb)
//      }
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
  class LoaderSerializerReader(filename: String) {

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

    def arrayOfInt(): Array[Int] = {
      val size = dos.readInt()
      val al = new Array[Int](size)
      (0 until size).foreach { i =>
        al(i) = dos.readInt()
      }
      al
    }

    def arrayOfLong(): Array[Long] = {
      val size = dos.readInt()
      val al = new Array[Long](size)
      (0 until size).foreach { i =>
        al(i) = dos.readLong()
      }
      al
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

  private def write(filename: String): LoaderSerializerWriter = {
    new LoaderSerializerWriter("%s/%s".format(directory, filename))
  }

  private def read(filename: String): LoaderSerializerReader = {
    new LoaderSerializerReader("%s/%s".format(directory, filename))
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
  def writeOrRead(filename: String, writeFn: (LoaderSerializerWriter => Unit),
                  readFn: (LoaderSerializerReader => Unit)) = {
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