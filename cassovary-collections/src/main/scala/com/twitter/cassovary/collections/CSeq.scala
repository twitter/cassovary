package com.twitter.cassovary.collections

import scala.collection.immutable
import scala.language.experimental.macros


/**
  * CSeq is fast and memory efficient equivalent of Scala's `immutable.IndexedSeq` with minimal
  * interface that is
  * 1) generic;
  * 2) specialized for `Int` and `Long`;
  * 3) does not use boxing for `Int` and `Long`;
  * 4) and uses arrays of primitive types underneath.
  *
  *
  * @tparam T type of element
  */
trait CSeq[@specialized(Int, Long) T] {
  def apply(idx: Int): T
  def length: Int

  /**
    * @return A sliced view of this CSeq.
    */
  def slice(from: Int, until: Int)(implicit csf: CSeqFactory[T]) : CSeq[T] = {
    val new_until = math.min(length, until)
    if (from >= new_until) {
      csf.empty
    } else {
      csf.fromCSeq(this, from, new_until)
    }
  }

  /**
    * @return Scala's `Indexed.Seq` that wraps this CSeq. Uses boxing.
    */
  def toSeq: CSeqWrapper[T] = new CSeqWrapper[T](this)

  def isEmpty = length == 0

  def foreach[U](f: T => U): Unit = {
    var i: Int = 0
    while (i < length) {
      f(apply(i))
      i = i + 1
    }
  }

  /** Methods below are not optimized */
  override def hashCode(): Int = toSeq.hashCode()

  override def equals(obj: scala.Any): Boolean = obj match {
    case cseq: CSeq[T] => toSeq.equals(cseq.toSeq)
    case _ => false
  }
}

class CSeqWrapper[T](underlying: CSeq[T]) extends immutable.IndexedSeq[T] {
  override def length: Int = underlying.length

  override def apply(idx: Int): T = underlying.apply(idx)
}

abstract class CSeqFactory[@specialized(Int, Long) T] {
  def ofDim(n: Int): CSeq[T]

  def fromCSeq(arr: CSeq[T], from: Int, until: Int): CSeq[T]

  def fromArray(arr: Array[T]): CSeq[T]

  def empty: CSeq[T]
}

/**
  * Companion object to CSeq. This object's apply methods should be used to
  * create new CSeqs.
  */
object CSeq {
  def ofDim[@specialized(Int, Long) T](n: Int)(implicit cSeqFactory: CSeqFactory[T]) = {
    cSeqFactory.ofDim(n)
  }

  def apply[@specialized(Int, Long) T](arr: Array[T])(implicit cSeqFactory: CSeqFactory[T]) = {
    cSeqFactory.fromArray(arr)
  }

  def apply[@specialized(Int, Long) T](arr: Array[T], from: Int, until: Int)
                                      (implicit cSeqFactory: CSeqFactory[T]): CSeq[T] = {
    cSeqFactory.fromCSeq(apply(arr), from, until)
  }


  def apply[@specialized(Int, Long) T](arr: CSeq[T], from: Int, until: Int)
                                      (implicit cSeqFactory: CSeqFactory[T]): CSeq[T] = {
    cSeqFactory.fromCSeq(arr, from, until)
  }

  def empty[@specialized(Int, Long) T](implicit cSeqFactory: CSeqFactory[T]) = {
    cSeqFactory.empty
  }

  object Implicits {
    // We use Scala Macros to avoid boilerplate code when defining
    // classes for specific types. Otherwise, we need to define CSeqFactory and
    // other classes for Int, Long and other types we want CSeqs to be specialized.
    //
    // This method is called fundep materialization.
    implicit def materializeCSeqFactory[T]: CSeqFactory[T] = macro impl[T]
    def impl[T: c.WeakTypeTag](c: scala.reflect.macros.blackbox.Context): c.Expr[CSeqFactory[T]] = {
      import c.universe._

      val sym = c.weakTypeOf[T]
      val className = internal.reificationSupport.freshTypeName("CSeq" + sym.toString + "$")
      val arrayWrapperName = internal.reificationSupport.freshTypeName("CSeq" + sym.toString +
        "ArrayWrapper$")
      val factoryClassName = internal.reificationSupport.freshTypeName("CFactory" + sym.toString
        + "$")

      val clazz = q"""
        class $className(underlying: CSeq[$sym], from: Int, until: Int)
            extends com.twitter.cassovary.collections.CSeq[$sym] {
          override def apply(idx: Int) = {
            if (idx >= length) {
              throw new IndexOutOfBoundsException(idx.toString)
            } else {
              underlying.apply(from + idx)
            }
          }

          override def length: Int = until - from
        }

        class $arrayWrapperName(underlying: Array[$sym])
            extends com.twitter.cassovary.collections.CSeq[$sym] {
          override def apply(idx: Int) = underlying.apply(idx)

          override def length = underlying.length
        }

        class $factoryClassName
            extends com.twitter.cassovary.collections.CSeqFactory[$sym] {
          override def ofDim(n: Int): CSeq[$sym] = new $arrayWrapperName(Array.ofDim[$sym](n))

          override def empty: CSeq[$sym] = new $arrayWrapperName(Array[$sym]())

          override def fromCSeq(arr: CSeq[$sym], from: Int, until: Int): CSeq[$sym] =
            new $className(arr, from, until)

          override def fromArray(arr: Array[$sym]): CSeq[$sym] = new $arrayWrapperName(arr)
        }
        new $factoryClassName()
      """

      c.Expr[CSeqFactory[T]](clazz)
    }
  }
}


