package com.twitter.cassovary.util

import java.util.concurrent.Executors
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await, ExecutionContext}

class ArrayBackedSetSpec extends WordSpec with Matchers {
  implicit val ecctxt = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

  "An ArrayBackedSet" when {

    "used by single thread" should {

      "perform set operations correctly" in {
        val set = new ArrayBackedSet(5)
        set.add(2)
        set.add(5)
        set.contains(2) shouldEqual true
        set.contains(5) shouldEqual true
        set.contains(3) shouldEqual false
        set.size shouldEqual 2
        set.iterator.toList shouldEqual List(2, 5)
        set.remove(2)
        set.remove(5)
        set.contains(5) shouldEqual false
        set.size shouldEqual 0
        set.iterator.isEmpty shouldEqual true
      }

    }

    "used by 4 threads concurrently" should {

      "perform concurrent add and read" in {
        val set = new ArrayBackedSet(50)
        val futures = (1 to 20) map { i =>
          Future {
            set.remove(i)
            set.add(i)
            set.contains(i) shouldEqual true
            set.add(i - 1)
            set.add(i + 1)
          }
        }
        Await.ready(Future.sequence(futures), Duration.Inf)
        (1 to 20) foreach { i =>
          set.contains(i) shouldEqual true
        }
        set.iterator.toList shouldEqual (0 to 21).toList
      }
    }
  }
}
