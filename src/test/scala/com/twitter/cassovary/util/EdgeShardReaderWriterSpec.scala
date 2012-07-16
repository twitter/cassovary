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
import com.google.common.util.concurrent.MoreExecutors
import java.util.concurrent.Executors
import actors.Actor
import java.util.concurrent.atomic.AtomicInteger
import net.lag.logging.Logger

class EdgeShardReaderWriterSpec extends Specification {
  val log = Logger.get
  val largeNumber = 1234567890
  val oneSequence = List(2000200200)
  val twoSequence = List(7, 7)
  val sevenSequence = List(132435, 243546, 354657, 999999999, 2000000000, 909909909, 1)
  "EdgeShardWriter and EdgeShardReader" should {
    "R/W 1 integer" in {
      val esw = new EdgeShardWriter("test.txt")
      esw.writeIntegersSequentially(List(largeNumber))
      esw.close
      val esr = new EdgeShardReader("test.txt")
      val intArray = new Array[Int](2)
      esr.readIntegersFromOffsetIntoArray(0, 1, intArray, 1)
      esr.close
      intArray(1) mustEqual largeNumber
    }
    "R/W 7 integers" in {
      val esw = new EdgeShardWriter("test.txt")
      esw.writeIntegersSequentially(sevenSequence)
      esw.close
      val esr = new EdgeShardReader("test.txt")
      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(0, 7, intArray, 3)
      esr.close
      (0 until 7).foreach { i => intArray(3+i) mustEqual sevenSequence(i) }
    }
    "W 3,3,1 integers, R 1 integer 7 times" in {
      val esw = new EdgeShardWriter("test.txt")
      esw.writeIntegersSequentially(sevenSequence)
      esw.writeIntegersSequentially(sevenSequence.slice(0,3))
      esw.writeIntegersSequentially(sevenSequence.slice(3,3))
      esw.writeIntegersSequentially(sevenSequence.slice(6,1))
      esw.close
      val esr = new EdgeShardReader("test.txt")
      val intArray = new Array[Int](10)
      (0 until 7).reverse.foreach { i =>
        esr.readIntegersFromOffsetIntoArray(i*4, 1, intArray, 3)
        intArray(3) mustEqual sevenSequence(i)
      }
      esr.close
    }
    "W 7 integers, seek 3 integers, read 3 integers, then 1 integer successfully" in {
      val esw = new EdgeShardWriter("test.txt")
      esw.writeIntegersSequentially(sevenSequence)
      esw.close
      val esr = new EdgeShardReader("test.txt")
      val intArray = new Array[Int](10)
      esr.readIntegersFromOffsetIntoArray(3*4, 3, intArray, 3)
      (0 until 3).foreach { i => intArray(3+i) mustEqual sevenSequence(3+i) }
      esr.readIntegersFromOffsetIntoArray(6*4, 1, intArray, 7)
      intArray(7) mustEqual sevenSequence(6)
      esr.close
    }

    "Concurrent writes at independent offsets preserve validity of file" in {
      case class Hello(i:Int)
      val a = new AtomicInteger()

      class WriteActor extends Actor {
        val esw = new EdgeShardWriter("test.txt")
        def act() {
          while (true) {
            receive {
              case Hello(x) => {
                esw.writeIntegersAtOffset(x * 4 * 2, Seq(x,x+1)) // Write 2 integers at some location
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
      val esr = new EdgeShardReader("test.txt")
      val intArray = new Array[Int](10000)
      esr.readIntegersFromOffsetIntoArray(8, 9998, intArray, 2)
      (1 until 5000).foreach { i =>
        intArray(i*2) mustEqual i
        intArray(i*2+1) mustEqual i+1
      }
    }
  }

  "EdgeShardsWriter and EdgeShardsReader" should {
    "Read and write 1 integer" in {
      val esw = new EdgeShardsWriter("test-shards", 5)
      esw.writeIntegersSequentially(5, oneSequence)
      esw.close
      val esr = new EdgeShardsReader("test-shards", 5)
      val intArray = new Array[Int](1)
      esr.readIntegersFromOffsetIntoArray(5, 0, 1, intArray, 0)
      esr.close
      intArray(0) mustEqual oneSequence(0)
    }
    "Read and write 3 sets of integers in different orders" in {
      val esw = new EdgeShardsWriter("test-shards", 5)
      esw.writeIntegersSequentially(1212121212, twoSequence)
      esw.writeIntegersSequentially(5, sevenSequence)
      val offset = esw.writeIntegersSequentially(999999990, oneSequence)
      esw.close
      val esr = new EdgeShardsReader("test-shards", 5)
      val intArray = new Array[Int](10)
      offset mustEqual 7 * 4
      esr.readIntegersFromOffsetIntoArray(999999990, 7*4, 1, intArray, 0)
      esr.readIntegersFromOffsetIntoArray(1212121212, 0, 2, intArray, 1)
      esr.readIntegersFromOffsetIntoArray(5, 0, 7, intArray, 3)
      val combined = oneSequence ::: twoSequence ::: sevenSequence
      (0 until 10).foreach { i => combined(i) mustEqual intArray(i) }
    }
  }
}
