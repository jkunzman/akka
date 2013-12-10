/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.ByteString
import com.typesafe.config.Config
import scala.util.DynamicVariable

/**
 * A global thread-local transaction for transaction tracing.
 */
object Transaction {
  val empty = ""

  val transaction = new DynamicVariable[String](empty)

  def value: String = transaction.value
  def value_=(newValue: String) = transaction.value = newValue

  def withValue[T](newValue: String)(thunk: ⇒ T) = transaction.withValue(newValue)(thunk)

  def clear(): Unit = transaction.value = empty
}

/**
 * Example tracer implementation that threads a transaction identifier
 * through message flows using the trace context and a thread-local.
 */
class TransactionTracer(config: Config) extends ContextOnlyTracer {
  def getContext(): Any = Transaction.value

  def setContext(context: Any): Unit =
    context match {
      case transaction: String ⇒ Transaction.value = transaction
      case _                   ⇒
    }

  def clearContext(): Unit = Transaction.clear()

  def serializeContext(context: Any): ByteString = {
    context match {
      case transaction: String ⇒ ByteString(transaction, "UTF-8")
      case _                   ⇒ ByteString.empty
    }
  }

  def deserializeContext(context: ByteString): Any = context.utf8String
}
