package com.twitter.cassovary.util.collections

import java.util.NoSuchElementException

import com.twitter.cassovary.util.collections.Implicits._
import org.scalatest.{Matchers, WordSpec}

/**
 * Created by szymonmatejczyk on 23.09.15.
 */
class FastQueueSpec extends WordSpec with Matchers {
  "FastQueue" should {
    "enqueue/deque elements" when {
      "using fifo" in {
        val q = FastQueue.fifo[Int]()
        q.isEmpty should be (true)
        q += 1
        q += 2
        q += 1
        q += 3

        q.deque() should equal (1)
        q.deque() should equal (2)
        q.deque() should equal (1)
        q.deque() should equal (3)

        q.isEmpty should be (true)

        intercept[NoSuchElementException](q.deque())
      }

      "using priority" in {
        val q = FastQueue.naturalPriority[Int]()
        q.isEmpty should be (true)
        q += 1
        q += 2
        q += 1
        q += 3

        q.deque() should equal (1)
        q.deque() should equal (1)
        q.deque() should equal (2)
        q.deque() should equal (3)

        q.isEmpty should be (true)

        intercept[NoSuchElementException](q.deque())
      }

      "using own priority" in {
        val q = FastQueue.priority[Int](Some(Order.by[Int, Int](-_)))
        q.isEmpty should be (true)
        q += 1
        q += 2
        q += 1
        q += 3

        q.deque() should equal (3)
        q.deque() should equal (2)
        q.deque() should equal (1)
        q.deque() should equal (1)

        q.isEmpty should be (true)

        intercept[NoSuchElementException](q.deque())
      }
    }
  }
}
