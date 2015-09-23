package com.twitter.cassovary.util.io

import org.scalatest.{Matchers, WordSpec}
import scala.reflect.runtime.universe._

class LabelsReaderSpec extends WordSpec with Matchers {
  val reader = new LabelsReader("cassovary-core/src/test/resources/graphs", "nodelabels_toy1")

  "Int label reader" should {
    "read int array-based labels" in {
      Seq((Some(false), Some(10)), (Some(true), Some(10)), (None, None)) foreach {
        case (isSparse, maxId) =>
          val labels = reader.read(isSparse, maxId)
          labels.get[String]("label1") shouldEqual None

          val label = labels.get[Int]("label1")
          label.isDefined shouldEqual true
          label.get.name shouldEqual "label1"
          label.get.tag shouldEqual typeTag[Int]
          label.get.toList.filter(_._2 != 0).sorted shouldEqual List((1, 1000), (2, 3333), (10, 2000))

          val label2 = labels.get[String]("label2")
          label2.isDefined shouldEqual true
          label2.get.name shouldEqual "label2"
          label2.get.tag shouldEqual typeTag[String]
          label2.get.toList.filter(_._2 != null).sorted shouldEqual List((2, "two"), (3, "three"), (10, "ten"))

      }
    }
  }

}
