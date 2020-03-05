package io.hops.monitoring.util

object RichOption {

  def notNull[T](x : T) = if (x == null) None else Some(x)

  implicit def richOption[T](x : Option[T]) = new {
    def ?![B](f : T => B) = notNull(f(x get))
    def ![B](f : T => B) = if (x isDefined) f(x get)
  }
}
