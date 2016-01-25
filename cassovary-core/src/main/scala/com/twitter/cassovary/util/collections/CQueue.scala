package com.twitter.cassovary.util.collections

import it.unimi.dsi.fastutil.PriorityQueue
import it.unimi.dsi.fastutil.ints.{IntArrayFIFOQueue, IntComparator, IntHeapPriorityQueue,
  IntPriorityQueue}
import it.unimi.dsi.fastutil.objects.{ObjectArrayFIFOQueue, ObjectArrayPriorityQueue}

/**
 * Specialized implementation of a queue. Queue allows adding elements and
 * removing them in a predefined order (defined in subclasses).
 *
 * Implemented methods are not boxed, when using with some `Int` and `Long`.
 */
trait CQueue[@specialized(Int, Long) T] {
  /**
   * @return Next element to be dequeued.
   */
  def first(): T

  /**
   *
   * @return Next element that is removed from the `CQueue`.
   */
  def deque(): T

  /**
   * Adds element to the `CQueue`.
   */
  def +=(elem: T)

  def isEmpty: Boolean
  def clear(): Unit
  def size(): Int
}

/**
 * FIFO [[CQueue]] - elements are added to the end of an underlying list and
 * removed from the beginning.
 */
trait FIFO[@specialized(Int, Long) T] extends CQueue[T] {
  /**
   * Adds an element at the beginning of the underlying list.
   */
  def enqueueFirst(elem: T)
}

object CQueue {
  import Implicits._
  /**
   * Creates specialized [[CQueue]] that is a FIFO queue.
   */
  def fifo[@specialized(Int, Long) T](): FIFO[T]= {
    implicitly[CQueueFactory[T]].fifo()
  }

  /**
   * Priority [[CQueue]] that uses default ordering (implicit) for `T`.
   */
  def naturalPriority[@specialized(Int, Long) T]()(implicit order: Order[T]): CQueue[T] = {
    implicitly[CQueueFactory[T]].priority(order)
  }

  /**
   * Priority [[CQueue]] that uses provided `order`.
   */
  def priority[@specialized(Int, Long) T](order: Order[T]): CQueue[T] = {
    implicitly[CQueueFactory[T]].priority(order)
  }

  trait LowerPriorityImplicit {
    private val factoryAny = new CQueueFactoryAny[AnyRef]()
    implicit def factoryAny[T]: CQueueFactory[T] = factoryAny.asInstanceOf[CQueueFactory[T]]
  }

  trait LowPriorityImplicit extends LowerPriorityImplicit {
    implicit val factoryInt: CQueueFactory[Int] = new CQueueFactoryInt()
  }

  object Implicits extends LowPriorityImplicit
}

class CQueueAny[T, QueueType <: PriorityQueue[T]](protected val underlying: QueueType)
    extends CQueue[T] {
  def +=(elem: T): Unit = underlying.enqueue(elem)
  def deque(): T = underlying.dequeue()
  override def first(): T = underlying.first()
  override def isEmpty: Boolean = underlying.isEmpty
  override def clear(): Unit = underlying.clear()
  override def size(): Int = underlying.size()
}

class CQueueInt[QueueType <: IntPriorityQueue](protected val underlying: QueueType)
    extends CQueue[Int] {
  override def +=(elem: Int): Unit = underlying.enqueue(elem)
  override def deque(): Int = underlying.dequeueInt()
  override def first(): Int = underlying.firstInt()
  override def isEmpty: Boolean = underlying.isEmpty
  override def clear(): Unit = underlying.clear()
  override def size(): Int = underlying.size()
}

abstract class CQueueFactory[T] {
  def fifo(): FIFO[T]
  def priority(): CQueue[T]
  def priority(order: Order[T]): CQueue[T]
}

private[collections] class CQueueFactoryAny[T] extends CQueueFactory[T] {
  def fifo(): FIFO[T] = new CQueueAny[T, ObjectArrayFIFOQueue[T]](
      new ObjectArrayFIFOQueue[T]()) with FIFO[T] {
    override def enqueueFirst(elem: T): Unit = underlying.enqueueFirst(elem)
  }

  def priority(order: Order[T]): CQueue[T] = {
      new CQueueAny[T, ObjectArrayPriorityQueue[T]](
        new ObjectArrayPriorityQueue[T](Order.ordering(order)))
  }

  override def priority(): CQueue[T] = throw new IllegalArgumentException("Ordering missing for T")
}

private[collections] class CQueueFactoryInt extends CQueueFactory[Int] {
  override def fifo() = new CQueueInt[IntArrayFIFOQueue](new IntArrayFIFOQueue()) with FIFO[Int] {
    def enqueueFirst(elem: Int): Unit = underlying.enqueueFirst(elem)
  }

  def priority(order: Order[Int]): CQueue[Int] = {
    val intComparator = new IntComparator {
      def compare(i: Int, i1: Int): Int = order.compare(i, i1)

      def compare(o1: Integer, o2: Integer): Int = {
        throw new IllegalStateException("Boxed comparison should not be used")
      }
    }
    new CQueueInt[IntHeapPriorityQueue](new IntHeapPriorityQueue(intComparator))
  }

  override def priority(): CQueue[Int] =
    new CQueueInt[IntHeapPriorityQueue](new IntHeapPriorityQueue())
}
