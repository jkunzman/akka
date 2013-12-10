/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor.{ ActorSystem, ActorRef }
import akka.util.{ ByteIterator, ByteString }
import scala.annotation.tailrec
import scala.collection.immutable

/**
 * Implementation of Tracer that delegates to multiple tracers.
 * A MultiTracer is only created when there are two or more tracers attached.
 * Trace contexts are stored as a sequence, and aligned with tracers when retrieved.
 * Efficient implementation using an array and manually inlined loops.
 */
class MultiTracer(val tracers: immutable.Seq[Tracer]) extends Tracer {
  implicit private val byteOrder = java.nio.ByteOrder.BIG_ENDIAN

  // DO NOT MODIFY THIS ARRAY
  private val _tracers: Array[Tracer] = tracers.toArray

  private val length: Int = _tracers.length

  def systemStarted(system: ActorSystem): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).systemStarted(system)
      i += 1
    }
  }

  def systemShutdown(system: ActorSystem): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).systemShutdown(system)
      i += 1
    }
  }

  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).actorTold(actorRef, message, sender)
      i += 1
    }
  }

  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).actorReceived(actorRef, message, sender)
      i += 1
    }
  }

  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).actorCompleted(actorRef, message, sender)
      i += 1
    }
  }

  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).remoteMessageSent(actorRef, message, size, sender)
      i += 1
    }
  }

  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).remoteMessageReceived(actorRef, message, size, sender)
      i += 1
    }
  }

  def getContext(): Any = {
    val builder = Vector.newBuilder[Any]
    var i = 0
    while (i < length) {
      builder += _tracers(i).getContext()
      i += 1
    }
    builder.result
  }

  def setContext(context: Any): Unit = {
    val contexts: Vector[Any] = context match {
      case v: Vector[_] ⇒ v
      case _            ⇒ Vector.empty[Any]
    }
    var i = 0
    while (i < length) {
      val ctx = if (contexts.isDefinedAt(i)) contexts(i) else Tracer.emptyContext
      _tracers(i).setContext(ctx)
      i += 1
    }
  }

  def clearContext(): Unit = {
    var i = 0
    while (i < length) {
      _tracers(i).clearContext()
      i += 1
    }
  }

  def serializeContext(context: Any): ByteString = {
    val builder = ByteString.newBuilder
    val contexts: Vector[Any] = context match {
      case v: Vector[_] ⇒ v
      case _            ⇒ Vector.empty[Any]
    }
    var i = 0
    while (i < length) {
      val ctx = if (contexts.isDefinedAt(i)) contexts(i) else Tracer.emptyContext
      val bytes = _tracers(i).serializeContext(ctx)
      builder.putInt(bytes.size)
      builder ++= bytes
      i += 1
    }
    builder.result
  }

  def deserializeContext(context: ByteString): Any = {
    val iterator = context.iterator
    val builder = Vector.newBuilder[Any]
    var i = 0
    while (i < length) {
      builder += _tracers(i).deserializeContext(getContextBytes(iterator))
      i += 1
    }
    builder.result
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
}
