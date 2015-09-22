package com.twitter.cassovary.util

import java.util.concurrent.ConcurrentHashMap
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap

import scala.collection.mutable
import scala.collection.convert.decorateAsScala._
import scala.reflect.ClassTag

object Int2ObjectMap {

  def apply[T : ClassTag](isSparse: Boolean, numKeysEstimate: Option[Int],
      maxId: Option[Int], isConcurrent: Boolean = true): mutable.Map[Int, T] = {
    if (isSparse) {
      if (isConcurrent) {
        numKeysEstimate match {
          case Some(n) => new ConcurrentHashMap[Int, T](n, 0.4f).asScala
          case None => new ConcurrentHashMap[Int, T]().asScala
        }
      } else {
        numKeysEstimate match {
          case Some(n) =>
            new Int2ObjectOpenHashMap[T](n, 0.4f).asInstanceOf[java.util.Map[Int, T]].asScala
          case None =>
            new Int2ObjectOpenHashMap[T]().asInstanceOf[java.util.Map[Int, T]].asScala
        }
      }
    } else {
      new ArrayBackedInt2ObjectMap[T](maxId.get)
    }
  }
}
