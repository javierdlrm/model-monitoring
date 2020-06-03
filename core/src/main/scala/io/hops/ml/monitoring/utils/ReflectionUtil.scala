package io.hops.ml.monitoring.utils

import scala.reflect.ClassTag

object ReflectionUtil {

  class PrivateMethodCaller[T: ClassTag](x: T, methodName: String) {
    def apply[R](_args: Any*): R = {
      val args = _args.map(_.asInstanceOf[AnyRef])

      def _parents: Stream[Class[_]] = Stream(scala.reflect.classTag[T].runtimeClass) #::: _parents.map(_.getSuperclass)

      val parents = _parents.takeWhile(_ != null).toList
      val methods = parents.flatMap(_.getDeclaredMethods)
      val method = methods.find(_.getName == methodName).getOrElse(throw new IllegalArgumentException("Method " + methodName + " not found"))
      method.setAccessible(true)
      method.invoke(x, args: _*).asInstanceOf[R]
    }
  }

  class p[T: ClassTag](x: T) {
    def call(method: scala.Symbol) = new PrivateMethodCaller[T](x, method.name)
  }

  def p[T: ClassTag](x: T): p[T] = new p[T](x)

  def ps[T <: AnyRef : ClassTag]: p[T] = new p[T](null.asInstanceOf[T])
}