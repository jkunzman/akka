/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor.{ ActorRef, ActorSystem }
import akka.util.ByteString
import com.typesafe.config.Config

/**
 * Tracer implementation that simply prints activity. Can be used for println debugging.
 */
class PrintTracer(config: Config) extends Tracer {
  def trace(message: String) = println("[trace] " + message)

  def systemStarted(system: ActorSystem): Unit =
    trace("system started")

  def systemShutdown(system: ActorSystem): Unit =
    trace("system shutdown")

  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any = {
    trace(s"actor told: $actorRef ! $message (sender = $sender)")
    Tracer.emptyContext
  }

  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit =
    trace(s"actor received: $actorRef ! $message (sender = $sender)")

  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit =
    trace(s"actor completed: $actorRef ! $message (sender = $sender)")

  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString = {
    trace(s"remote message sent: $actorRef ! $message (size = $size bytes, sender = $sender)")
    ByteString.empty
  }

  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, contextBytes: ByteString): Unit =
    trace(s"remote message received: $actorRef ! $message (size = $size bytes, sender = $sender)")

  def remoteMessageCompleted(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit =
    trace(s"remote message completed: $actorRef ! $message (size = $size bytes, sender = $sender)")
}
