package com.twitter.cassovary.util.collections

import java.util

import it.unimi.dsi.fastutil.ints._
import it.unimi.dsi.fastutil.objects._

import scala.collection.generic.CanBuildFrom
import scala.collection.mutable
import scala.{specialized => sp}

/**
 * Specialized, fast and memory efficient implementation of mutable map.
 *
 * Provides basic operations only. See [[FastMapWrapper]] for how to use
 * it as a [[mutable.Map]].
 *
 * Use constructor provided in the companion object.
 */
trait FastMap[@sp(Int, Long, AnyRef) A, @sp(Int, Long, AnyRef) B] {

  /**
   * Adds key-value pair to the map.
   */
  def +=(key: A, value: B): this.type

  /**
   * Removes key-value pair with a given key.
   */
  def -=(key: A): this.type

  /**
   * Get is optimized for retrieving elements contained in the Map.
   *
   * If the element is not in the map, returns `0` of given type (or false, null).
   */
  def get(key: A): B

  def clear(): Unit

  def contains(key: A): Boolean

  def size(): Int

  /**
   * Wraps as a [[mutable.Map]].
   */
  def asScala(): mutable.Map[A, B] = new FastMapWrapper[A, B](this)

  /**
   * Returns iterator over key, value pairs stored in the map.
   *
   * Used only inside to conform to Scala's collections.
   */
  private[collections] def iterator: Iterator[(A, B)]
}

/**
 * Implements incrementing value specified by a given key.
 */
trait AddTo[@sp(Int, Long, AnyRef) A] {
  def addTo(key: A, increment: Int)
}

/**
 * Marks a [[FastMap]] with an iterator that preserves insertion order.
 */
trait InsertionOrderIterator

/**
 * Features creation of specialized FastMaps.
 */
object FastMap {
  /**
   * @return Specialized FastMap created for key type `A` and value type `B`.
   */
  def apply[@sp(Int, Long, AnyRef) A, @sp(Int, Long, AnyRef) B]()
      (implicit impl: FastMapFactory[A, B, FastMap[A, B]]): FastMap[A, B] = {
    impl()
  }

  /**
   * @return FastMap created for given key type `A`, value type `B` and FastMap type `MapType`.
   */
  def applyFor[@sp(Int, Long, AnyRef) A, @sp(Int, Long, AnyRef) B, MapType <: FastMap[A, B]]()
      (implicit impl: FastMapFactory[A, B, MapType]): MapType = {
    impl()
  }

  trait FastMapFactory[@sp(Int, Long, AnyRef) A, @sp(Int, Long, AnyRef) B,
      MapType <: FastMap[A, B]] {
    def apply(): MapType
  }

  final class FastMapFactoryAnyAny[A, B] extends FastMapFactory[A, B, FastMap[A, B]] {
    def apply() = new FastMapAnyAny[A, B]()
  }

  final class FastMapFactoryAnyInt[A] extends FastMapFactory[A, Int, FastMap[A, Int]] {
    def apply() = new FastMapAnyInt[A, Object2IntOpenHashMap[A]](new Object2IntOpenHashMap[A]())
  }

  final class FastMapFactoryAnyIntWithAdd[A]
      extends FastMapFactory[A, Int, FastMap[A, Int] with AddTo[A]] {
    def apply() = new FastMapAnyInt[A, Object2IntOpenHashMap[A]](new Object2IntOpenHashMap[A]())
      with AddTo[A] {
      override def addTo(key: A, increment: Int): Unit = underlying.addTo(key, increment)
    }
  }

  final class FastMapFactoryAnyIntWithInsertionOrder[A] extends FastMapFactory[A, Int, FastMap[A,
    Int] with InsertionOrderIterator] {
    def apply() = new FastMapAnyInt[A, Object2IntArrayMap[A]](new Object2IntArrayMap[A]())
      with InsertionOrderIterator
  }

  final class FastMapFactoryIntAny[B] extends FastMapFactory[Int, B, FastMapInt[B]] {
    def apply() = new FastMapInt[B](new Int2ObjectOpenHashMap[B]())
  }

  final class FastMapFactoryIntInt extends FastMapFactory[Int, Int, FastMap[Int, Int]] {
    def apply() = new FastMapIntInt[Int2IntOpenHashMap](new Int2IntOpenHashMap())
  }

  final class FastMapFactoryIntIntWithAddTo extends FastMapFactory[Int, Int, FastMap[Int, Int]
    with AddTo[Int]] {
    def apply() = new FastMapIntInt[Int2IntOpenHashMap](new Int2IntOpenHashMap()) with AddTo[Int] {
      override def addTo(key: Int, increment: Int): Unit = underlying.addTo(key, increment)
    }
  }

  final class FastMapFactoryIntIntWithInsertionOrderIterator extends FastMapFactory[Int, Int,
    FastMap[Int, Int] with InsertionOrderIterator] {
    def apply() = new FastMapIntInt[Int2IntArrayMap](new Int2IntArrayMap()) with
      InsertionOrderIterator
  }

  final class FastMapFactoryIntBool extends FastMapFactory[Int, Boolean, FastMap[Int, Boolean]] {
    def apply() = new FastMapIntBool()
  }

  trait LowestPriorityImplicit {
    private val factoryAnyAny = new FastMapFactoryAnyAny[AnyRef, AnyRef]()

    implicit def factoryAnyAny[A, B]: FastMapFactory[A, B, FastMap[A, B]] =
      factoryAnyAny.asInstanceOf[FastMapFactoryAnyAny[A, B]]

    private val factoryAnyIntWithAddTo = new FastMapFactoryAnyIntWithAdd[AnyRef]
    implicit def factoryAnyIntWithAddTo[A]: FastMapFactoryAnyIntWithAdd[A] =
      factoryAnyIntWithAddTo.asInstanceOf[FastMapFactoryAnyIntWithAdd[A]]

    private val factoryAnyIntWithInsertionOrder = new FastMapFactoryAnyIntWithInsertionOrder[AnyRef]
    implicit def factoryAnyIntWithInsertionOrder[A]: FastMapFactoryAnyIntWithInsertionOrder[A] =
      factoryAnyIntWithInsertionOrder.asInstanceOf[FastMapFactoryAnyIntWithInsertionOrder[A]]
  }

  trait LowerPriorityImplicit extends LowestPriorityImplicit {
    private val factoryAnyInt = new FastMapFactoryAnyInt[AnyRef]()
    implicit def factoryAnyInt[A]: FastMapFactoryAnyInt[A] =
      factoryAnyInt.asInstanceOf[FastMapFactoryAnyInt[A]]

    private val factoryIntAny = new FastMapFactoryIntAny[AnyRef]()
    implicit def factoryIntAny[B]: FastMapFactoryIntAny[B] =
      factoryIntAny.asInstanceOf[FastMapFactoryIntAny[B]]

    implicit val factoryIntIntWithAddTo = new FastMapFactoryIntIntWithAddTo()
    implicit val factoryIntIntWithInsertionOrderIterator =
      new FastMapFactoryIntIntWithInsertionOrderIterator()
  }

  trait LowPriorityImplicit extends LowerPriorityImplicit {
    implicit val factoryIntBool = new FastMapFactoryIntBool()
    implicit val factoryIntInt = new FastMapFactoryIntInt()
  }

  object Implicits extends LowerPriorityImplicit
}

/**
 * Wraps `FastMap` to use as Scala's Map.
 */
class FastMapWrapper[A, B](underlying: FastMap[A, B]) extends mutable.Map[A, B]
  with mutable.MapLike[A, B, FastMapWrapper[A, B]] {
  import FastMap.Implicits._

  override def +=(kv: (A, B)): FastMapWrapper.this.type = {
    underlying += (kv._1, kv._2)
    this
  }

  override def -=(key: A): FastMapWrapper.this.type = {
    underlying -= key
    this
  }

  override def get(key: A): Option[B] = {
    if (underlying.contains(key)) {
      Some(underlying.get(key))
    } else {
      None
    }
  }

  override def iterator: Iterator[(A, B)] = underlying.iterator

  override def empty = new FastMapWrapper[A, B](FastMap[A, B])
}

/**
 * Provides methods for FastMapWrapper integration with Scala's collections framework.
 */
object FastMapWrapper {
  import FastMap.Implicits._
  def empty[A, B] = new FastMapWrapper[A, B](FastMap[A, B])

  def newBuilder[A, B]: mutable.Builder[(A, B), FastMapWrapper[A, B]] = {
    new mutable.MapBuilder[A, B, FastMapWrapper[A, B]](empty[A, B])
  }

  implicit def canBuildFrom[A, B]: CanBuildFrom[FastMapWrapper[_, _], (A, B), FastMapWrapper[A, B]] =
    new CanBuildFrom[FastMapWrapper[_, _], (A, B), FastMapWrapper[A, B]] {
      def apply(from: FastMapWrapper[_, _]) = newBuilder[A, B]
      def apply() = newBuilder[A, B]
    }
}

/*
 * Manual specialization for certain types.
 */

trait JavaUtilMapWrapper[@sp(Int, Long, AnyRef) A, @sp(Int, Long, AnyRef) B] {
  protected def utilMap: java.util.Map[A, B]

  def size() = utilMap.size()
  def clear() = utilMap.clear()
}

class FastMapAnyAny[@sp(Int, Long, AnyRef) A, @sp(Int, Long, AnyRef) B]
  (protected val underlying: Object2ObjectMap[A, B] = new Object2ObjectOpenHashMap[A, B]())
  extends FastMap[A, B] with JavaUtilMapWrapper[A, B]{

  def +=(key: A, value: B): this.type = {
    underlying.put(key, value)
    this
  }

  def -=(key: A): this.type = {
    underlying.remove(key)
    this
  }

  def get(key: A): B = {
    underlying.get(key)
  }

  def contains(key: A) = underlying.containsKey(key)

  private[collections] def iterator: Iterator[(A, B)] = new Iterator[(A, B)] {
    val it = underlying.object2ObjectEntrySet().iterator()

    override def hasNext: Boolean = it.hasNext

    override def next(): (A, B) = {
      val next = it.next()
      (next.getKey, next.getValue)
    }
  }

  override protected def utilMap: util.Map[A, B] = underlying
}

class FastMapAnyInt[@sp(Int, Long, AnyRef) A, MapType <: Object2IntMap[A]](
    protected val underlying: MapType
  ) extends FastMap[A, Int] with JavaUtilMapWrapper[A, Int] {

  def +=(key: A, value: Int): this.type = {
    underlying.put(key, value)
    this
  }

  def -=(key: A): this.type = {
    underlying.remove(key)
    this
  }

  def get(key: A): Int = {
    underlying.getInt(key)
  }

  override def contains(key: A) = underlying.containsKey(key)

  override private[collections] def iterator: Iterator[(A, Int)] = new Iterator[(A, Int)] {
    val it = underlying.object2IntEntrySet().iterator()

    override def hasNext: Boolean = it.hasNext

    override def next(): (A, Int) = {
      val next = it.next()
      (next.getKey, next.getIntValue)
    }
  }

  override protected def utilMap: util.Map[A, Int] = underlying.asInstanceOf[util.Map[A, Int]]
}

/**
 * Specialization for `Int -> B` map.
 */
class FastMapInt[@sp(Int, Long, AnyRef) B]
  (private[collections] val underlying: Int2ObjectMap[B] = new Int2ObjectOpenHashMap[B]())
  extends FastMap[Int, B] {

  override def +=(key: Int, value: B): this.type = {
    underlying.put(key, value)
    this
  }

  override def -=(key: Int): this.type = {
    underlying.remove(key)
    this
  }

  override def get(key: Int): B = {
    underlying.get(key)
  }

  override def clear(): Unit = {
    underlying.clear()
  }

  override def contains(key: Int) = underlying.containsKey(key)

  override private[collections] def iterator: Iterator[(Int, B)] = new Iterator[(Int, B)] {
    val it = underlying.int2ObjectEntrySet().iterator()

    override def hasNext: Boolean = it.hasNext

    override def next(): (Int, B) = {
      val next = it.next()
      (next.getIntKey, next.getValue)
    }
  }

  override def size(): Int = underlying.size()
}

/**
 * Specialization for `Int -> Int` map.
 */
class FastMapIntInt[MapType <: Int2IntMap]
  (protected val underlying: MapType)
  extends FastMap[Int, Int] {

  override def +=(key: Int, value: Int): this.type = {
    underlying.put(key, value)
    this
  }

  override def -=(key: Int): this.type = {
    underlying.remove(key)
    this
  }

  override def get(key: Int): Int = {
    underlying.get(key)
  }

  override def clear(): Unit = {
    underlying.clear()
  }

  override def contains(key: Int) = underlying.containsKey(key)

  override private[collections] def iterator: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    val it = underlying.int2IntEntrySet().iterator()

    override def hasNext: Boolean = it.hasNext

    override def next(): (Int, Int) = {
      val next = it.next()
      (next.getIntKey, next.getIntValue)
    }
  }

  override def size(): Int = underlying.size()
}

/**
 * Specialization for Int -> Bool map.
 */
class FastMapIntBool
  (private[collections] val underlying: Int2BooleanMap = new Int2BooleanOpenHashMap())
  extends FastMap[Int, Boolean] {

  override def +=(key: Int, value: Boolean): this.type = {
    underlying.put(key, value)
    this
  }

  override def -=(key: Int): this.type = {
    underlying.remove(key)
    this
  }

  override def get(key: Int): Boolean = {
    underlying.get(key)
  }

  override def clear(): Unit = {
    underlying.clear()
  }

  override def contains(key: Int) = underlying.containsKey(key)

  override private[collections] def iterator: Iterator[(Int, Boolean)] =
    new Iterator[(Int, Boolean)] {
      val it = underlying.int2BooleanEntrySet().iterator()

      override def hasNext: Boolean = it.hasNext

      override def next(): (Int, Boolean) = {
        val next = it.next()
        (next.getIntKey, next.getBooleanValue)
    }
  }

  override def size(): Int = underlying.size()
}
