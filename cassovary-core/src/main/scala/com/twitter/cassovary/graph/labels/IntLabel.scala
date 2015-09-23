package com.twitter.cassovary.graph.labels

import it.unimi.dsi.fastutil.ints.{Int2ObjectOpenHashMap, Int2IntOpenHashMap}

import scala.collection.convert.decorateAsScala._
import scala.collection.mutable
import scala.reflect.runtime.universe._
import scala.reflect.ClassTag

/**
 * Specialized labels for int ids (label keys). Label values can be anything.
 * @param numEstimate An estimate of the number of keys
 * @tparam L  the type of the label value
 */
class IntLabel[L: ClassTag](val name: String,
    val numEstimate: Option[Int] = None)(implicit val tag: TypeTag[L])
    extends Label[Int, L] {

  private val underlying: mutable.Map[Int, L] = {
    if (tag == typeTag[Int]) {
      val map1 = numEstimate match {
        case Some(n) => new Int2IntOpenHashMap(n, 0.4f)
        case None => new Int2IntOpenHashMap()
      }
      map1.asInstanceOf[java.util.Map[Int, L]].asScala
    } else {
      val map1 = numEstimate match {
        case Some(n) => new Int2ObjectOpenHashMap[L](n, 0.4f)
        case None => new Int2ObjectOpenHashMap[L]()
      }
      map1.asInstanceOf[java.util.Map[Int, L]].asScala
    }
  }

  def get(id: Int): Option[L] = underlying.get(id)

  def +=(kv: (Int, L)) = {
    underlying += kv
    this
  }

  def -=(id: Int) = {
    underlying -= id
    this
  }

  def iterator = underlying.iterator

}
