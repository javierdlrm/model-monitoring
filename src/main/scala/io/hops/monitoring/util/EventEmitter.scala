package io.hops.monitoring.util

import RichOption._
import scala.collection.mutable

class EventEmitter[T] {
  private var subscribers = new mutable.PriorityQueue[EventHandler[T]]()

  def invoke(key: T) : Unit = {
    for (subscriber <- subscribers.clone.dequeueAll)
      subscriber.invoke(key)
  }

  def subscribe(handler: EventHandler[T]): Unit = subscribers.enqueue(handler)

  def unsubscribe(key : T, handler: Option[T => Unit] = None) : Unit = {
    subscribers = subscribers filter ((eh: EventHandler[T]) => {
      val keyMismatch = eh.key != key
      if (handler isDefined) keyMismatch && handler.get != eh.condition.get
      else keyMismatch
    })
  }

  def count(key: T): Int = subscribers.count(_.key == key)
}

case class EventHandler[T](key: T, handler: T => Unit, condition: Option[T => Boolean] = None, priority: Int = 0) extends Ordered[EventHandler[T]] {
  def invoke(key: T): Unit = {
    condition!(cond =>
      if (cond(key)) handler(key))
  }
  override def compare(that: EventHandler[T]): Int = this.priority - that.priority
}

