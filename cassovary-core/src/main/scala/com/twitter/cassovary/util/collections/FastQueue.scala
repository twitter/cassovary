package com.twitter.cassovary.util.collections

import it.unimi.dsi.fastutil.PriorityQueue
import it.unimi.dsi.fastutil.ints.{IntComparator, IntHeapPriorityQueue, IntArrayFIFOQueue, IntPriorityQueue}
import it.unimi.dsi.fastutil.objects.{ObjectArrayFIFOQueue, ObjectArrayPriorityQueue}

abstract class FastQueue[@specialized(Int, Long) T] {
  def first(): T
  def +=(elem: T)
  def deque(): T
  def isEmpty: Boolean
  def clear(): Unit
  def size(): Int
}

trait FIFO[@specialized(Int, Long) T] {
  def enqueueFirst(elem: T)
}

object FastQueue {

  def fifo[@specialized(Int, Long) T]()(implicit impl: FastQueueFactory[T]): FastQueue[T] with FIFO[T]= {
    impl.fifo()
  }

  def naturalPriority[@specialized(Int, Long) T]()(
    implicit order: Order[T],
    impl: FastQueueFactory[T]
  ): FastQueue[T] = {
    impl.priority(Some(order))
  }

  def priority[@specialized(Int, Long) T](order: Option[Order[T]])(implicit impl: FastQueueFactory[T]): FastQueue[T] = {
    impl.priority(order)
  }

  trait LowerPriorityImplicit {
    private val factoryAny = new FastQueueFactoryAny[AnyRef]()
    implicit def factoryAny[T]: FastQueueFactory[T] = factoryAny.asInstanceOf[FastQueueFactory[T]]
  }

  trait LowPriorityImplicit extends LowerPriorityImplicit {
    implicit val factoryInt: FastQueueFactory[Int] = new FastQueueFactoryInt()
  }

  object Implicits extends LowPriorityImplicit
}

class FastQueueAny[T, QueueType <: PriorityQueue[T]](protected val underlying: QueueType)
    extends FastQueue[T] {
  def +=(elem: T): Unit = underlying.enqueue(elem)

  def deque(): T = underlying.dequeue()

  def isEmpty: Boolean = underlying.isEmpty

  def clear() = underlying.clear()

  override def size(): Int = underlying.size()

  override def first(): T = underlying.first()
}

class FastQueueInt[QueueType <: IntPriorityQueue](protected val underlying: QueueType)
    extends FastQueue[Int]{
  override def +=(elem: Int): Unit = underlying.enqueue(elem)

  override def deque(): Int = underlying.dequeueInt()

  override def isEmpty: Boolean = underlying.isEmpty

  override def clear(): Unit = underlying.clear()

  override def size(): Int = underlying.size()

  override def first(): Int = underlying.firstInt()
}

abstract class FastQueueFactory[T] {
  def fifo(): FastQueue[T] with FIFO[T]
  def priority(order: Option[Order[T]]): FastQueue[T]
}

private[collections] class FastQueueFactoryAny[T] extends FastQueueFactory[T] {
  override def fifo(): FastQueue[T] with FIFO[T] = new FastQueueAny[T, ObjectArrayFIFOQueue[T]](
      new ObjectArrayFIFOQueue[T]()) with FIFO[T] {
    override def enqueueFirst(elem: T): Unit = underlying.enqueueFirst(elem)
  }

  override def priority(order: Option[Order[T]]): FastQueue[T] = order match {
    case Some(o) =>
      new FastQueueAny[T, ObjectArrayPriorityQueue[T]](
        new ObjectArrayPriorityQueue[T](Order.ordering(o)))
    case None =>
      throw new IllegalArgumentException("Order must be specified for type T")
  }
}

private[collections] class FastQueueFactoryInt extends FastQueueFactory[Int] {
  override def fifo() = new FastQueueInt[IntArrayFIFOQueue](new IntArrayFIFOQueue()) with FIFO[Int] {
    override def enqueueFirst(elem: Int): Unit = underlying.enqueueFirst(elem)
  }

  override def priority(order: Option[Order[Int]]): FastQueue[Int] = order match {
    case Some(o) =>
      val intComparator = new IntComparator {
        override def compare(i: Int, i1: Int): Int = o.compare(i, i1)

        override def compare(o1: Integer, o2: Integer): Int = {
          throw new IllegalStateException("Boxed comparison should not be used")
        }
      }
      new FastQueueInt[IntHeapPriorityQueue](new IntHeapPriorityQueue(intComparator))
    case None =>
      new FastQueueInt[IntHeapPriorityQueue](new IntHeapPriorityQueue())
  }
}


