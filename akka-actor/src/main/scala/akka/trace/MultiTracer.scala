/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor.{ ActorSystem, ActorRef }
import akka.util.{ ByteIterator, ByteString }
import scala.annotation.tailrec

/**
 * Implementation of Tracer that delegates to multiple tracers.
 * A MultiTracer is only created when there are two or more tracers attached.
 * Trace contexts are stored as a sequence, and aligned with tracers when retrieved.
 * Looping and mapping is manually inlined with while loops or tail recursive inner methods.
 */
class MultiTracer(val tracers: List[Tracer]) extends Tracer {
  implicit val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  val emptyContexts = List.fill[Any](tracers.length)(Tracer.emptyContext)

  def systemStarted(system: ActorSystem): Unit = {
    var ts = tracers
    while (ts.nonEmpty) {
      ts.head.systemStarted(system)
      ts = ts.tail
    }
  }

  def systemShutdown(system: ActorSystem): Unit = {
    var ts = tracers
    while (ts.nonEmpty) {
      ts.head.systemShutdown(system)
      ts = ts.tail
    }
  }

  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any = {
    val builder = List.newBuilder[Any]
    var ts = tracers
    while (ts.nonEmpty) {
      builder += ts.head.actorTold(actorRef, message, sender)
      ts = ts.tail
    }
    builder.result
  }

  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit = {
    @tailrec def process(tracers: List[Tracer], contexts: List[Any]): Unit = {
      if (tracers.nonEmpty) {
        tracers.head.actorReceived(actorRef, message, sender, contexts.head)
        process(tracers.tail, contexts.tail)
      }
    }
    context match {
      case contexts: List[_] ⇒ process(tracers, contexts)
      case _                 ⇒ process(tracers, emptyContexts)
    }
  }

  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit = {
    var ts = tracers
    while (ts.nonEmpty) {
      ts.head.actorCompleted(actorRef, message, sender)
      ts = ts.tail
    }
  }

  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString = {
    val builder = ByteString.newBuilder
    @tailrec def process(tracers: List[Tracer], contexts: List[Any]): Unit = {
      if (tracers.nonEmpty) {
        val bytes = tracers.head.remoteMessageSent(actorRef, message, size, sender, contexts.head)
        builder.putInt(bytes.size)
        builder ++= bytes
        process(tracers.tail, contexts.tail)
      }
    }
    context match {
      case contexts: List[_] ⇒ process(tracers, contexts)
      case _                 ⇒ process(tracers, emptyContexts)
    }
    builder.result
  }

  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: ByteString): Unit = {
    val iterator = context.iterator
    var ts = tracers
    while (ts.nonEmpty) {
      ts.head.remoteMessageReceived(actorRef, message, size, sender, getContextBytes(iterator))
      ts = ts.tail
    }
  }

  def getContextBytes(iterator: ByteIterator): ByteString = {
    try {
      val length = iterator.getInt
      val bytes = new Array[Byte](length)
      iterator.getBytes(bytes)
      ByteString(bytes)
    } catch {
      case _: java.util.NoSuchElementException ⇒ ByteString.empty
    }
  }

  def remoteMessageCompleted(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit = {
    var ts = tracers
    while (ts.nonEmpty) {
      ts.head.remoteMessageCompleted(actorRef, message, size, sender)
      ts = ts.tail
    }
  }
}
