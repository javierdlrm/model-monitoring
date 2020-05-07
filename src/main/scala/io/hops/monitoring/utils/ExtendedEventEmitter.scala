package io.hops.monitoring.utils

import RichOption._
import scala.collection.mutable

class ExtendEventEmitter[T, A] {
  private var subscribers = new mutable.PriorityQueue[ExtendedEventHandler[T, A]]()

  def invoke(key: T, args: A): Unit = {
    for (subscriber <- subscribers.clone.dequeueAll)
      subscriber.invoke(key, args)
  }

  def subscribe(handler: ExtendedEventHandler[T, A]): Unit = subscribers.enqueue(handler)

  def unsubscribe(key: T, handler: Option[(T, A) => Unit] = None): Unit = {
    subscribers = subscribers filter ((eh: ExtendedEventHandler[T, A]) => {
      val keyMismatch = eh.key != key
      if (handler isDefined) keyMismatch && handler.get != eh.condition.get
      else keyMismatch
    })
  }

  def count(key: T): Int = subscribers.count(_.key == key)
}

case class ExtendedEventHandler[T, A](key: T, handler: (T, A) => Unit, condition: Option[T => Boolean] = None, priority: Int = 0) extends Ordered[ExtendedEventHandler[T, A]] {
  def invoke(key: T, args: A): Unit = {
    condition ! (cond =>
      if (cond(key)) handler(key, args))
  }

  override def compare(that: ExtendedEventHandler[T, A]): Int = this.priority - that.priority
}
