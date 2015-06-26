package com.twitter.cassovary.util.io

import java.io.{File, RandomAccessFile}
import java.nio.file.NoSuchFileException

import org.scalatest.{Matchers, WordSpec}

class MemoryMappedIntLongSourceSpec extends WordSpec with Matchers {
  "MemoryMappedIntLongSource" should {
    "read Ints and Longs from a multi-GB file" in {
       // create a file with ints and long at various locations
       val intValues = Map(
         0L -> 12,
         4L -> -34,
         // Make sure ints on both sides of a buffer boundary are read correctly
         (1L << 30) - 4 -> 56,
         (1L << 30) -> 12345678
       )
      val longValues = Map(
        (1L << 32) - 8 -> 98L,
        (1L << 32) -> -76L,
        (1L << 32) + 128 -> 123456789012345L
      )
      val file = File.createTempFile("IntLongData", "dat")
      val out = new RandomAccessFile(file, "rw")
      for ((i, value) <- intValues) {
        out.seek(i)
        out.writeInt(value)
      }
      for ((i, value) <- longValues) {
        out.seek(i)
        out.writeLong(value)
      }
      out.close()

      // Verify that the source can read the written data
      val source = new MemoryMappedIntLongSource(file)
      for ((i, value) <- intValues) {
        source.getInt(i) shouldEqual (value)
      }
      for ((i, value) <- longValues) {
        source.getLong(i) shouldEqual (value)
      }

      an[ArrayIndexOutOfBoundsException] should be thrownBy {
        source.getInt(1L << 33)
      }
      an[IndexOutOfBoundsException] should be thrownBy {
        source.getLong((1L << 32) + 129L)
      }

      file.deleteOnExit()
    }

    " throw an error given an invalid filename" in {
      a[NoSuchFileException] should be thrownBy {
        new MemoryMappedIntLongSource("nonexistant_file_4398219812437401")
      }
    }
  }
}
