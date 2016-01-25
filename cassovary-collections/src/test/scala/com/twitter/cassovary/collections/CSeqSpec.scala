package com.twitter.cassovary.collections

import org.scalatest.{Matchers, WordSpec}

class CSeqSpec extends WordSpec with Matchers {
  import CSeq.Implicits._

  "CSeq" should {
    "behave like an array" when {
      "created for int" in {
        val s = Array.ofDim[Int](5)
        s(0) = 0
        s(1) = 1

        val cSeq = CSeq(s)

        cSeq.length shouldEqual 5
        cSeq(0) shouldEqual 0
        cSeq(1) shouldEqual 1
      }

      "created for long" in {
        val s = Array.ofDim[Long](5)
        s(0) = 0
        s(1) = 1

        val cSeq = CSeq(s)

        cSeq.length shouldEqual 5
        cSeq(0) shouldEqual 0
        cSeq(1) shouldEqual 1
      }
    }
    "allow enumeration using foreach" when {
      "CSeq is empty" in {
        val s = CSeq(Array[Int]())
        s.foreach {
          x => fail
        }
      }
      "CSeq is nonempty" in {
        val s = CSeq(Array(1, 2, 3))
        var cur = 1
        s.foreach {
          x => {
            x shouldEqual cur
            cur += 1
          }
        }

        cur shouldEqual 4
      }
    }
    "implement slicing" in {
      val s = CSeq(Array(1, 2, 3, 4, 5))

      var cur = 2
      s.slice(1, 3).foreach {
        x => {
          x shouldEqual cur
          cur += 1
        }
      }
      cur shouldEqual 4
    }
    "not allow to access memory out of scope" in {
      val s = CSeq(Array(1, 2, 3, 4, 5))
      val slice = s.slice(1, 3)

      an[IndexOutOfBoundsException] should be thrownBy {
        slice(2)
      }
    }
    "implement equals correctly" when {
      "comparing two CSeq[Int]" in {
        val s1 = CSeq[Int](Array(1, 2, 3))
        val s2 = CSeq[Int](Array(1, 2, 3))

        s1 shouldEqual s2
      }

      "comparing CSeq[Int] with CSeq[Long]" in {
        val s1 = CSeq[Int](Array(1, 2, 3))
        val s2 = CSeq[Long](Array[Long](1, 2, 3))

        s1 shouldEqual s2
      }
    }
  }
}
