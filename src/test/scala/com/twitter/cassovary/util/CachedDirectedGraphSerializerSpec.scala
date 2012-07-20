package com.twitter.cassovary.util

import org.specs.Specification
import scala.collection.mutable
import java.util.concurrent.atomic.AtomicLong

class CachedDirectedGraphSerializerSpec extends Specification  {
  "CachedDirectedGraphSerializer" should {
    val serializer = new CachedDirectedGraphSerializer("temp-cached/spectest", true)

    "Read and write a BitSet properly" in {
      val bs = new mutable.BitSet(999)
      bs(989) = true
      bs(0) = true

      serializer.writeOrRead("bitset.txt", { writer =>
        writer.bitSet(bs).close
      }, { reader => () })

      serializer.writeOrRead("bitset.txt", { writer => () }, { reader =>
        val bos = reader.bitSet()
        bos(989) mustEqual true
        bs(0) mustEqual true
        (0 to 1000).filterNot(i => i == 0 || i == 989).foreach { i => bs(i) mustEqual false }
      })
    }

    "Read and write a Array of Long,Int tuples properly" in {
      val bs = new Array[(Long, Int)](1000)
      bs(244) = (999999999999L, 12)
      bs(999) = (1234567890L, 51)

      serializer.writeOrRead("array.txt", { writer =>
        writer.arrayOfLongInt(bs).close
      }, { reader => () })

      serializer.writeOrRead("array.txt", { writer => () }, { reader =>
        val bos = reader.arrayOfLongInt()
        bos(244) mustEqual (999999999999L, 12)
        bos(999) mustEqual (1234567890L, 51)
        (0 until 1000).filterNot(i => i == 244 || i == 999).foreach { i => bos(i) must beNull }
      })
    }

    "Read and write an atomic long array properly" in {
      val kaka = new Array[AtomicLong](21)
      (0 until 21).foreach { i => kaka(i) = new AtomicLong() }
      kaka(5).set(52L)

      serializer.writeOrRead("atomic.txt", { writer =>
        writer.atomicLongArray(kaka).close
      }, { reader => () })

      serializer.writeOrRead("atomic.txt", { writer => () }, { reader =>
        val koko = reader.atomicLongArray()
        koko(5).get mustEqual 52L
        (0 until 21).filterNot(i => i == 5).foreach { i => koko(i).get mustEqual 0L }
      })
    }

    "Read and write a few values properly" in {
      val ali = new Array[(Long, Int)](1000)
      val bs = new mutable.BitSet(999)
      val aal = new Array[AtomicLong](1024)
      (0 until 1000).foreach { i => ali(i) = (i * 1424354657L, i) }
      (0 until 999).foreach { i => bs(i) = (i % 11) == 0 }
      (0 until 21).foreach { i => aal(i) = new AtomicLong(); aal(i).set(i * 999999999L) }

      serializer.writeOrRead("few.txt", { writer =>
        writer.arrayOfLongInt(ali).bitSet(bs).atomicLongArray(aal).close
      }, { reader => () })

      serializer.writeOrRead("few.txt", { writer => () }, { reader =>
        val a = reader.arrayOfLongInt()
        val b = reader.bitSet()
        val c = reader.atomicLongArray()
        (0 until 1000).foreach { i => a(i) mustEqual (i * 1424354657L, i) }
        (0 until 999).foreach { i => b(i) mustEqual (i % 11) == 0 }
        (0 until 21).foreach { i => c(i).get mustEqual i * 999999999L }
      })
    }
  }
}
