package com.twitter.cassovary.util.collections

import org.scalatest.{WordSpec, Matchers}

class FastMapSpec extends WordSpec with Matchers {
  def newIntIntMap() = {
    import FastMap.Implicits._
    FastMap[Int, Int]()
  }

  "FastMap" should {
    "allow inserting and retrieving elements" in {
      val map = newIntIntMap()

      map += (1, 2)
      map += (2, 3)
      map += (3, 4)

      map -= 2

      map.get(1) should equal (2)
      map.get(3) should equal (4)
      map.contains(2) should equal (false)
    }
    "return default value when key is not in the map" when {
      "using int values" in {
        val map = newIntIntMap()

        map.get(1) should equal(0)
        map.get(2) should equal(0)

        map +=(1, 2)
        map.get(1) should equal(2)
      }
      "using object values" in {
        import FastMap.Implicits._
        val map = FastMap[Int, String]()

        map += (1, "a")

        map.get(1) should equal ("a")
        map.contains(1) should equal (true)

        map.get(2) should equal (null)
        map.contains(2) should equal (false)
      }
    }
  }
}
