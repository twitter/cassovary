package com.twitter.cassovary.util

import java.util.concurrent.Executors
import org.scalatest.{Matchers, WordSpec}

import scala.concurrent.duration.Duration
import scala.concurrent.{Future, Await, ExecutionContext}

class ArrayBackedInt2ObjectMapSpec extends WordSpec with Matchers {
  implicit val ecctxt = ExecutionContext.fromExecutorService(Executors.newFixedThreadPool(8))

  "An Int->String map" when {

    "used by single thread" should {

      "perform map operations correctly" in {
        val m = new ArrayBackedInt2ObjectMap[String](100)
        m += 1 -> "1"
        m += 2 -> "20"
        m.get(1) shouldEqual Some("1")
        m.get(2) shouldEqual Some("20")
        m.get(0) shouldEqual None
        m.get(100) shouldEqual None

        m -= 1
        m.get(1) shouldEqual None
        m.iterator.toList shouldEqual List((2, "20"))
      }
    }

    "used by multiple threads" should {

      "perform concurrent add, remove and get operations correctly" in {
        val m = new ArrayBackedInt2ObjectMap[String](100)
        val futures = (2 to 100) map { i =>
          Future {
            m -= i
            m += i -> i.toString
            m.get(i) shouldEqual Some(i.toString)
            m += (i - 1) -> (i - 1).toString
          }
        }
        Await.ready(Future.sequence(futures), Duration.Inf)
        m.get(0) shouldEqual None
        (1 to 100) foreach { i =>
          m.get(i) shouldEqual Some(i.toString)
        }
      }
    }
  }
}