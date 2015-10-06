package com.twitter.cassovary.util.collections

/**
 * Specialized typeclass for describing ordering between elements.
 *
 * Based on spire implementation.
 */
trait Order[@specialized(Int, Long) A] {
  def eqv(x: A, y: A): Boolean = compare(x, y) == 0
  def gt(x: A, y: A): Boolean = compare(x, y) > 0
  def lt(x: A, y: A): Boolean = compare(x, y) < 0
  def gteqv(x: A, y: A): Boolean = compare(x, y) >= 0
  def lteqv(x: A, y: A): Boolean = compare(x, y) <= 0

  def min(x: A, y: A): A = if (lt(x, y)) x else y
  def max(x: A, y: A): A = if (gt(x, y)) x else y
  def compare(x: A, y: A): Int

  /**
   * Defines an order on `B` by mapping `B` to `A` using `f` and using `A`s
   * order to order `B`.
   */
  def on[@specialized(Int, Long) B](f: B => A): Order[B] = new MappedOrder(this)(f)

  /**
   * Defines an ordering on `A` where all arrows switch direction.
   */
  def reverse: Order[A] = new ReversedOrder(this)
}

private[collections] class MappedOrder[@specialized(Int, Long) A, @specialized(Int, Long) B]
    (order: Order[B])(f: A => B) extends Order[A] {
  def compare(x: A, y: A): Int = order.compare(f(x), f(y))
}

private[collections] class ReversedOrder[@specialized(Int, Long) A](order: Order[A])
    extends Order[A] {
  def compare(x: A, y: A): Int = order.compare(y, x)
}

object Order {
  @inline final def apply[A](implicit o: Order[A]): Order[A] = o

  def by[@specialized(Int, Long) A, @specialized(Int, Long) B](f: A => B)
    (implicit o: Order[B]): Order[A] = o .on(f)

  def from[@specialized(Int, Long) A](f: (A, A) => Int): Order[A] = new Order[A] {
    def compare(x: A, y: A): Int = f(x, y)
  }

  def ordering[A](implicit o: Order[A]): Ordering[A] = new Ordering[A] {
    def compare(x: A, y: A): Int = o.compare(x, y)
  }

  implicit object IntOrder extends Order[Int] {
    def compare(x: Int, y: Int) =
      if (x < y) -1
      else if (x == y) 0
      else 1
  }

  implicit object LongOrder extends Order[Long] {
    def compare(x: Long, y: Long) =
      if (x < y) -1
      else if (x == y) 0
      else 1
  }
}
