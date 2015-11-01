package com.twitter.cassovary.util.collections

import java.util.NoSuchElementException

import org.scalatest.{Matchers, WordSpec}

class CQueueSpec extends WordSpec with Matchers {
  def verifyQueue(q: CQueue[Int], dequeSeq: Seq[Int]): Unit = {
    q.isEmpty should be (true)

    q += 1
    q += 2
    q += 1
    q += 3

    q.isEmpty should be (false)

    dequeSeq.foreach {
      e => q.deque() should equal (e)
    }
    q.isEmpty should be (true)

    intercept[NoSuchElementException](q.deque())

    // Check that fastutils were used
    try {
      q.deque()
    } catch {
      case e: Throwable =>
        e.getStackTrace.head.getClassName.contains("it.unimi.dsi.fastutil") shouldEqual (true)
    }
  }

  "CQueue" should {
    "enqueue/deque elements" when {
      "using fifo" in {
        val q = CQueue.fifo[Int]()
        verifyQueue(q, Seq(1, 2, 1, 3))
      }

      "using priority" in {
        val q = CQueue.naturalPriority[Int]()
        verifyQueue(q, Seq(1, 1, 2, 3))
      }

      "using own priority" in {
        val q = CQueue.priority[Int](Order.by[Int, Int](-_))
        verifyQueue(q, Seq(3, 2, 1, 1))
      }
    }
  }
}
