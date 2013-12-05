/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor.{ ActorSystem, ActorRef, DynamicAccess, ExtendedActorSystem }
import akka.util.ByteString
import com.typesafe.config.Config
import scala.collection.immutable
import scala.reflect.ClassTag

object Tracer {
  /**
   * Empty placeholder (null) for when there is no trace context.
   */
  val emptyContext: Any = null

  /**
   * Create the tracer(s) for an actor system.
   *
   * Tracer classes must extend akka.trace.Tracer and have a public constructor
   * which is empty or optionally accepts a com.typesafe.config.Config parameter.
   * The config object is the same one as used to create the actor system.
   *
   * If there are no tracers then a default empty implementation with final methods
   * is used (akka.trace.NoTracer).
   * If there is exactly one tracer, then it is created and will be called directly.
   * If there are two or more tracers, then an akka.trace.MultiTracer is created to
   * delegate to the individual tracers and coordinate the trace contexts.
   */
  def apply(tracers: immutable.Seq[String], config: Config, dynamicAccess: DynamicAccess): Tracer = {
    tracers.length match {
      case 0 ⇒ new NoTracer
      case 1 ⇒ create(dynamicAccess, config)(tracers.head)
      case _ ⇒ new MultiTracer(tracers.map(create(dynamicAccess, config)).toList)
    }
  }

  /**
   * Create a tracer dynamically from a fully qualified class name.
   * Tracer constructors can optionally accept the actor system config.
   */
  def create(dynamicAccess: DynamicAccess, config: Config)(fqcn: String): Tracer = {
    val configArg = List(classOf[Config] -> config)
    dynamicAccess.createInstanceFor[Tracer](fqcn, configArg).recoverWith({
      case _: NoSuchMethodException ⇒ dynamicAccess.createInstanceFor[Tracer](fqcn, Nil)
    }).get
  }

  /**
   * Access an attached tracer by class. Will return null if there is no matching tracer.
   */
  def access[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): T = {
    val tracer = system match {
      case actorSystem: ExtendedActorSystem ⇒
        actorSystem.tracer match {
          case multi: MultiTracer ⇒ multi.tracers find tracerClass.isInstance getOrElse null
          case single             ⇒ if (tracerClass isInstance single) single else null
        }
      case _ ⇒ null
    }
    tracer.asInstanceOf[T]
  }

  /**
   * Find an attached tracer by class, returning an Option.
   */
  def find[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): Option[T] =
    Option(access(system, tracerClass))

  /**
   * Access an attached tracer by class.
   * Will throw IllegalArgumentException if there is no matching tracer.
   */
  def apply[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): T =
    access(system, tracerClass) match {
      case null   ⇒ throw new IllegalArgumentException(s"Trying to access non-existent tracer [$tracerClass]")
      case tracer ⇒ tracer
    }

  /**
   * Access an attached tracer by implicit class tag.
   * Will throw IllegalArgumentException if there is no matching tracer.
   */
  def apply[T <: Tracer](system: ActorSystem)(implicit tag: ClassTag[T]): T =
    apply(system, tag.runtimeClass.asInstanceOf[Class[T]])

  /**
   * Check whether an attached tracer exists, matching by class.
   */
  def exists[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): Boolean =
    access(system, tracerClass) ne null

  /**
   * Java API: Access an attached tracer by class.
   * Will throw IllegalArgumentException if there is no matching tracer.
   */
  def get[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): T =
    apply(system, tracerClass)
}

/**
 * The Akka trace SPI.
 *
 * A context object is returned by some methods and will be propagated across threads.
 * In some implementations the context object will not be needed and can simply be null.
 * For remote messages the trace context needs to be serialized to bytes.
 */
abstract class Tracer {
  /**
   * Record actor system started (at end of start).
   */
  def systemStarted(system: ActorSystem): Unit

  /**
   * Record actor system shutdown (after shutdown).
   * Tracer cleanup and shutdown should also happen at this point.
   */
  def systemShutdown(system: ActorSystem): Unit

  /**
   * Record actor told (at message send).
   * Returns a context object which will be attached to the message send,
   * and passed on the corresponding call to `actorReceived`.
   */
  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any

  /**
   * Record actor received (at beginning of message processing).
   * Passes the context object attached by `actorTold`.
   */
  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit

  /**
   * Record actor completed (at end of message processing).
   */
  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit

  /**
   * Record remote message sent.
   * Also return the serialized trace context, if used, otherwise an empty byte string.
   */
  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString

  /**
   * Record remote message received.
   * The serialized trace context from `remoteMessageSent` is passed in.
   */
  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: ByteString): Unit

  /**
   * Record remote message completed (at end of remote message processing).
   */
  def remoteMessageCompleted(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit
}

/**
 * Default implementation of Tracer that does nothing. Final methods.
 */
final class NoTracer extends Tracer {
  final def systemStarted(system: ActorSystem): Unit = ()
  final def systemShutdown(system: ActorSystem): Unit = ()
  final def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any = Tracer.emptyContext
  final def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit = ()
  final def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit = ()
  final def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString = ByteString.empty
  final def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: ByteString): Unit = ()
  final def remoteMessageCompleted(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit = ()
}

/**
 * Implementation of Tracer that does nothing by default. Select methods can be overridden.
 */
class EmptyTracer extends Tracer {
  def systemStarted(system: ActorSystem): Unit = ()
  def systemShutdown(system: ActorSystem): Unit = ()
  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any = Tracer.emptyContext
  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit = ()
  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit = ()
  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString = ByteString.empty
  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: ByteString): Unit = ()
  def remoteMessageCompleted(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef): Unit = ()
}
