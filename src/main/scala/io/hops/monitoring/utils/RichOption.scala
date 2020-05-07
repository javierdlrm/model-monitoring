package io.hops.monitoring.utils

object RichOption {

  def notNull[T](x: T): Option[T] = if (x == null) None else Some(x)

  implicit def richOption[T](x: Option[T]): Object {
    def ?![B](f: T => B): Option[B]

    def ![B](f: T => B): Any
  } = new {
    def ?![B](f: T => B): Option[B] = notNull(f(x get))

    def ![B](f: T => B): Any = if (x isDefined) f(x get)
  }
}
