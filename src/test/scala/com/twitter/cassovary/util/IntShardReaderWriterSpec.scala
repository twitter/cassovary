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

import org.specs.Specification
import actors.Actor
import java.util.concurrent.atomic.AtomicInteger
import net.lag.logging.Logger

class IntShardReaderWriterSpec extends Specification {
  val log = Logger.get
  val largeNumber = 1234567890
  val oneSequence = List(2000200200)
  val twoSequence = List(7, 7)
  val sevenSequence = List(132435, 243546, 354657, 999999999, 2000000000, 909909909, 1)
  "SingleIntShardWriter and SingleIntShardReader" should {

    var tempFile = ""
    doBefore {
      tempFile = FileUtils.getTempFilename
    }

    "R/W 1 integer" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersSequentially(List(largeNumber))
      esw.close
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](2)
      esr.readIntegersFromOffsetIntoArray(0, 1, intArray, 1)
      esr.close
      intArray(1) mustEqual largeNumber
    }
    "R/W 7 integers" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersSequentially(sevenSequence)
      esw.close
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(0, 7, intArray, 3)
      esr.close
      (0 until 7).foreach { i => intArray(3+i) mustEqual sevenSequence(i) }
    }
    "W 3,3,1 integers, R 1 integer 7 times" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersSequentially(sevenSequence)
      esw.writeIntegersSequentially(sevenSequence.slice(0,3))
      esw.writeIntegersSequentially(sevenSequence.slice(3,3))
      esw.writeIntegersSequentially(sevenSequence.slice(6,1))
      esw.close
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](10)
      (0 until 7).reverse.foreach { i =>
        esr.readIntegersFromOffsetIntoArray(i, 1, intArray, 3)
        intArray(3) mustEqual sevenSequence(i)
      }
      esr.close
    }
    "W 7 integers, seek 3 integers, read 3 integers, then 1 integer successfully" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersSequentially(sevenSequence)
      esw.close
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(3, 3, intArray, 3)
      (0 until 3).foreach { i => intArray(3+i) mustEqual sevenSequence(3+i) }
      esr.readIntegersFromOffsetIntoArray(6, 1, intArray, 7)
      intArray(7) mustEqual sevenSequence(6)
      esr.close
    }

    "Concurrent writes at independent offsets preserve validity of file" in {
      case class Hello(i:Int)
      val a = new AtomicInteger()

      class WriteActor extends Actor {
        val esw = new SingleIntShardWriter(tempFile)
        def act() {
          while (true) {
            receive {
              case Hello(x) => {
                esw.writeIntegersAtOffset(x * 2, Seq(x,x+1)) // Write 2 integers at some location
                a.incrementAndGet()
              }
            }
          }
        }
        def close = esw.close
      }
      val actors = (0 until 10).map { i =>
        val a = new WriteActor()
        a.start()
        a
      }
      (1 until 5000).reverse.foreach { i => actors(i % 10) ! Hello(i) }
      while(a.get() != 4999) {
        log.info("Concurrent writes at %s".format(a.get()))
        Thread.sleep(100)
      }
      actors.foreach { a => a.close }
      // Read and ensure that everything was written correctly
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](10000)
      esr.readIntegersFromOffsetIntoArray(2, 9998, intArray, 2)
      (1 until 5000).foreach { i =>
        intArray(i*2) mustEqual i
        intArray(i*2+1) mustEqual i+1
      }
      esr.close
    }

    "Non-sequential writes work" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersAtOffset(6, Seq(5,6))
      esw.writeIntegersAtOffset(1, Seq(999))
      esw.writeIntegersAtOffset(8, Seq(42))
      esw.close
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](4)
      esr.readIntegersFromOffsetIntoArray(1, 1, intArray, 0)
      esr.readIntegersFromOffsetIntoArray(6, 3, intArray, 1)
      val actualSeq = Seq(999, 5, 6, 42)
      (0 until 4).foreach { i => intArray(i) mustEqual actualSeq(i) }
      esr.close
    }

    "Reading and writing nothing doesn't mess up" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersAtOffset(6, Seq(5, 7))
      esw.writeIntegersAtOffset(6, Seq())
      esw.writeIntegersAtOffset(6, Seq(6))
      esw.writeIntegersAtOffset(6, Seq())
      esw.close
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](2)
      esr.readIntegersFromOffsetIntoArray(5, 2, intArray, 0)
      esr.readIntegersFromOffsetIntoArray(6, 2, intArray, 0)
      esr.readIntegersFromOffsetIntoArray(7, 0, intArray, 0)
      intArray(0) mustEqual 6
      intArray(1) mustEqual 7
      esr.close
    }

    "Multiple writes by different writers doesn't mess up" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.writeIntegersAtOffset(2, Seq(9, 8))
      esw.close

      val ess = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](5)
      ess.readIntegersFromOffsetIntoArray(2, 2, intArray, 0)
      intArray(0) mustEqual 9
      intArray(1) mustEqual 8
      ess.close

      val esv = new SingleIntShardWriter(tempFile)
      esv.writeIntegersAtOffset(0, Seq(5, 6, 7))
      esv.writeIntegersAtOffset(4, Seq(9))
      esv.close

      val esr = new SingleIntShardReader(tempFile)
      esr.readIntegersFromOffsetIntoArray(2, 2, intArray, 0)
      intArray(0) mustEqual 7
      intArray(1) mustEqual 8
      esr.close
    }

    "Successfully make a large file of the right size" in {
      val esw = new SingleIntShardWriter(tempFile)
      esw.allocate(1000000)
      esw.length mustEqual 1000000
      esw.close
    }
  }

  "SingleMemIntShardWriter" should {
    "Write like SingleIntShardWriter" in {
      val tempFile = FileUtils.getTempFilename
      val esw = new SingleMemIntShardWriter(10)
      val its = (0 until 10).toArray
      esw.writeIntegersAtOffsetFromOffset(5, its, 5, 5)
      esw.writeIntegersAtOffsetFromOffset(0, its, 0, 5)
      esw.writeToShard(tempFile)
      val esr = new SingleIntShardReader(tempFile)
      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(0, 10, intArray, 0)
      (0 until 10).foreach { i => intArray(i) mustEqual i }
      esr.close
    }
  }

  "MemEdgeShardWriters" should {
    "Work in rounds" in {
      val tempDir = FileUtils.getTempDirectoryName
      val its = new Array[Array[Int]](8)
      (0 until 8).foreach { i =>
        its(i) = new Array[Int](i)
        (0 until i).foreach { j => its(i)(j) = j }
      }
      val shardSizes = Array[Int](4,6,8,10)
      val msw = new MemIntShardsWriter(tempDir, 4, shardSizes, 2)
      msw.startRound(0)
      msw.writeIntegersAtOffsetFromOffset(0, 0, its(0), 0, 0)
      msw.writeIntegersAtOffsetFromOffset(1, 0, its(1), 0, 1)
      msw.writeIntegersAtOffsetFromOffset(4, 0, its(4), 0, 4)
      msw.writeIntegersAtOffsetFromOffset(5, 1, its(5), 0, 5)
      msw.endRound
      msw.startRound(1)
      msw.writeIntegersAtOffsetFromOffset(2, 0, its(2), 0, 2)
      msw.writeIntegersAtOffsetFromOffset(3, 0, its(3), 0, 3)
      msw.writeIntegersAtOffsetFromOffset(6, 2, its(6), 0, 6)
      msw.writeIntegersAtOffsetFromOffset(7, 3, its(7), 0, 7)
      msw.endRound
      val esr = new IntShardsReader(tempDir, 4)

      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(1, 0, 1, intArray, 0)
      intArray(0) mustEqual 0
      esr.readIntegersFromOffsetIntoArray(6, 2, 6, intArray, 0)
      intArray(3) mustEqual 3
      intArray(4) mustEqual 4
      intArray(5) mustEqual 5
      esr.readIntegersFromOffsetIntoArray(7, 3, 7, intArray, 1)
      intArray(1) mustEqual 0
      intArray(7) mustEqual 6
      esr.close
    }
  }

  "IntShardsWriter and IntShardsReader" should {

    var tempDir = ""
    doBefore {
      tempDir = FileUtils.getTempDirectoryName
    }
    "Read and write 1 integer" in {
      val esw = new IntShardsWriter(tempDir, 5)
      esw.writeIntegersSequentially(5, oneSequence)
      esw.close
      val esr = new IntShardsReader(tempDir, 5)
      val intArray = new Array[Int](1)
      esr.readIntegersFromOffsetIntoArray(5, 0, 1, intArray, 0)
      esr.close
      intArray(0) mustEqual oneSequence(0)
    }
    "Read and write 3 sets of integers in different orders" in {
      val esw = new IntShardsWriter(tempDir, 5)
      esw.writeIntegersSequentially(1212121212, twoSequence)
      esw.writeIntegersSequentially(5, sevenSequence)
      val offset = esw.writeIntegersSequentially(999999990, oneSequence)
      esw.close
      val esr = new IntShardsReader(tempDir, 5)
      val intArray = new Array[Int](10)
      offset mustEqual 7
      esr.readIntegersFromOffsetIntoArray(999999990, 7, 1, intArray, 0)
      esr.readIntegersFromOffsetIntoArray(1212121212, 0, 2, intArray, 1)
      esr.readIntegersFromOffsetIntoArray(5, 0, 7, intArray, 3)
      val combined = oneSequence ::: twoSequence ::: sevenSequence
      (0 until 10).foreach { i => combined(i) mustEqual intArray(i) }
      esr.close
    }
  }

  "MultiDirMemIntShardsWriter and MultiDirIntShardsReader" should {
    val directories = Array(FileUtils.getTempDirectoryName, FileUtils.getTempDirectoryName)

    "Work in rounds" in {
      val its = new Array[Array[Int]](8)
      (0 until 8).foreach { i =>
        its(i) = new Array[Int](i)
        (0 until i).foreach { j => its(i)(j) = j }
      }
      val shardSizes = Array(4, 6, 8, 10)
      val msw = new MultiDirMemIntShardsWriter(directories, 4, shardSizes, 2)
      msw.startRound(0)
      msw.writeIntegersAtOffsetFromOffset(0, 0, its(0), 0, 0)
      msw.writeIntegersAtOffsetFromOffset(1, 0, its(1), 0, 1)
      msw.writeIntegersAtOffsetFromOffset(4, 0, its(4), 0, 4)
      msw.writeIntegersAtOffsetFromOffset(5, 1, its(5), 0, 5)
      msw.endRound
      msw.startRound(1)
      msw.writeIntegersAtOffsetFromOffset(2, 0, its(2), 0, 2)
      msw.writeIntegersAtOffsetFromOffset(3, 0, its(3), 0, 3)
      msw.writeIntegersAtOffsetFromOffset(6, 2, its(6), 0, 6)
      msw.writeIntegersAtOffsetFromOffset(7, 3, its(7), 0, 7)
      msw.endRound

      val esr = new MultiDirIntShardsReader(directories, 4)
      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(1, 0, 1, intArray, 0)
      intArray(0) mustEqual 0
      esr.readIntegersFromOffsetIntoArray(6, 2, 6, intArray, 0)
      intArray(3) mustEqual 3
      intArray(4) mustEqual 4
      intArray(5) mustEqual 5
      esr.readIntegersFromOffsetIntoArray(7, 3, 7, intArray, 1)
      intArray(1) mustEqual 0
      intArray(7) mustEqual 6
      esr.close
    }

  }

  "MultiDirIntShardsWriter and MultiDirIntShardsReader" should {
    val directories = Array(FileUtils.getTempDirectoryName, FileUtils.getTempDirectoryName)

    "Work" in {
      // Write some random numbers
      val esw = new MultiDirIntShardsWriter(directories, 4)
      (0 to 10).foreach { i =>
        val a = (0 to i).map{ j => j * i }.toArray
        esw.writeIntegersAtOffset(i, i * 11, a)
      }
      esw.close

      // Read those random numbers back
      val esr = new MultiDirIntShardsReader(directories, 4)
      val intArray = new Array[Int](11)
      (0 to 10).foreach { i =>
        esr.readIntegersFromOffsetIntoArray(i, i * 11, i + 1, intArray, 0)
        (0 to i).foreach { j => intArray(j) mustEqual j * i }
      }
    }

  }
}
