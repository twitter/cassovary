package com.twitter.cassovary.util.collections
import org.scalatest.{Matchers, WordSpec}

class ReusableArrayBufferSpec extends WordSpec with Matchers {

  "Reusable Array buffer" should {
    "appear as a simple array buffer" when {
      "used once" in {
        val rub = new ReusableArrayBuffer(1)
        rub.length shouldEqual 0
        rub += 10
        rub.length shouldEqual 1
        rub.toList shouldEqual List(10)
        rub += 20
        rub.length shouldEqual 2
        rub.toList shouldEqual List(10, 20)
        rub += 30
        rub.length shouldEqual 3
        rub(0) = 4
        rub.toList shouldEqual List(4, 20, 30)
        rub.toArray(0) shouldEqual Array(4, 20, 30)
        rub.toArray(1) shouldEqual Array(20, 30)
        rub.toArray(2) shouldEqual Array(30)
      }

      "reused a few times" in {
        val rub = new ReusableArrayBuffer(2)
        for (i <- 0 to 3) rub += i
        rub.toArray(0) shouldEqual Array(0, 1, 2, 3)
        rub.clear()
        rub.length shouldEqual 0
        rub += 100
        rub.toList shouldEqual List(100)
        rub += 200
        rub.toArray(1) shouldEqual Array(200)
      }
    }

  }

}
