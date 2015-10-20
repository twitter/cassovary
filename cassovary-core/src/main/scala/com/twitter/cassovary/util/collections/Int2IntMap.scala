package com.twitter.cassovary.util.collections


import it.unimi.dsi.fastutil.ints.Int2IntOpenHashMap

import scala.collection.mutable
import scala.collection.convert.decorateAsScala._

object Int2IntMap {

  def apply(isSparse: Boolean, numKeysEstimate: Option[Int],
      maxId: Option[Int]): mutable.Map[Int, Int] = {
    if (isSparse) {
        numKeysEstimate match {
          case Some(n) =>
            new Int2IntOpenHashMap(n, 0.4f).asInstanceOf[java.util.Map[Int, Int]].asScala
          case None =>
            new Int2IntOpenHashMap().asInstanceOf[java.util.Map[Int, Int]].asScala
        }
    } else {
      new ArrayBackedInt2IntMap(maxId.get)
    }
  }
}
