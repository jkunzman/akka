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
class TransactionTracer(config: Config) extends Tracer {
  final def systemStarted(system: ActorSystem): Unit = ()
  final def systemShutdown(system: ActorSystem): Unit = ()

  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any =
    Transaction.value

  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit =
    context match {
      case transaction: String ⇒ Transaction.value = transaction
      case _                   ⇒
    }

  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit =
    Transaction.clear()

  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString = {
    context match {
      case transaction: String ⇒ ByteString(transaction, "UTF-8")
      case _                   ⇒ ByteString.empty
    }
  }

  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: ByteString): Unit =
    Transaction.value = context.utf8String

  def remoteMessageCompleted(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit =
    Transaction.clear()
}
