/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor.{ ActorSystem, ActorRef, DynamicAccess, ExtendedActorSystem }
import akka.util.ByteString
import com.typesafe.config.Config
import scala.collection.immutable
import scala.reflect.ClassTag

/**
 * Tracers are attached to actor systems using the `akka.tracers` configuration option,
 * specifying a list of fully qualified class names of tracer implementations. For example:
 *
 * {{{
 * akka.tracers = ["com.example.SomeTracer"]
 * }}}
 *
 * Tracer classes must extend [[akka.trace.Tracer]] and have a public constructor
 * which is empty or optionally accepts a [[com.typesafe.config.Config]] parameter.
 * The config object is the same one as used to create the actor system.
 *
 * There are methods to access an attached tracer implementation on an actor system,
 * for tracers that provide user APIs.
 *
 * Accessing a tracer in Scala:
 * {{{
 * Tracer[SomeTracer](system) // throws exception if not found
 *
 * Tracer.find[SomeTracer](system) // returns Option
 * }}}
 *
 * Accessing a tracer in Java:
 * {{{
 * Tracer.exists(system, SomeTracer.class); // returns boolean
 *
 * Tracer.get(system, SomeTracer.class); // throws exception if not found
 * }}}
 */
object Tracer {
  /**
   * Empty placeholder (null) for when there is no trace context.
   */
  val emptyContext: Any = null

  /**
   * INTERNAL API. Determine whether or not tracing is enabled.
   */
  private[akka] def enabled(tracers: immutable.Seq[String], config: Config): Boolean = tracers.nonEmpty

  /**
   * INTERNAL API. Create the tracer(s) for an actor system.
   *
   * Tracer classes must extend [[akka.trace.Tracer]] and have a public constructor
   * which is empty or optionally accepts a [[com.typesafe.config.Config]] parameter.
   * The config object is the same one as used to create the actor system.
   *
   *  - If there are no tracers then a default empty implementation with final methods
   *    is used ([[akka.trace.NoTracer]]).
   *  - If there is exactly one tracer, then it is created and will be called directly.
   *  - If there are two or more tracers, then an [[akka.trace.MultiTracer]] is created to
   *    delegate to the individual tracers and coordinate the trace contexts.
   */
  private[akka] def apply(tracers: immutable.Seq[String], config: Config, dynamicAccess: DynamicAccess): Tracer = {
    tracers.length match {
      case 0 ⇒ new NoTracer
      case 1 ⇒ create(dynamicAccess, config)(tracers.head)
      case _ ⇒ new MultiTracer(tracers map create(dynamicAccess, config))
    }
  }

  /**
   * INTERNAL API. Create a tracer dynamically from a fully qualified class name.
   * Tracer constructors can optionally accept the actor system config.
   */
  private[akka] def create(dynamicAccess: DynamicAccess, config: Config)(fqcn: String): Tracer = {
    val configArg = List(classOf[Config] -> config)
    dynamicAccess.createInstanceFor[Tracer](fqcn, configArg).recoverWith({
      case _: NoSuchMethodException ⇒ dynamicAccess.createInstanceFor[Tracer](fqcn, Nil)
    }).get
  }

  /**
   * Access an attached tracer by class. Returns null if there is no matching tracer.
   */
  def access[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): T = {
    val tracer = system match {
      case actorSystem: ExtendedActorSystem ⇒
        actorSystem.tracer match {
          case multi: MultiTracer ⇒ (multi.tracers find tracerClass.isInstance).orNull
          case single             ⇒ if (tracerClass isInstance single) single else null
        }
      case _ ⇒ null
    }
    tracer.asInstanceOf[T]
  }

  /**
   * Find an attached tracer by class. Returns an Option.
   */
  def find[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): Option[T] =
    Option(access(system, tracerClass))

  /**
   * Find an attached tracer by implicit class tag. Returns an Option.
   */
  def find[T <: Tracer](system: ActorSystem)(implicit tag: ClassTag[T]): Option[T] =
    find(system, tag.runtimeClass.asInstanceOf[Class[T]])

  /**
   * Access an attached tracer by class.
   *
   * @throws IllegalArgumentException if there is no matching tracer
   */
  def apply[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): T =
    access(system, tracerClass) match {
      case null   ⇒ throw new IllegalArgumentException(s"Trying to access non-existent tracer [$tracerClass]")
      case tracer ⇒ tracer
    }

  /**
   * Access an attached tracer by implicit class tag.
   *
   * @throws IllegalArgumentException if there is no matching tracer
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
   *
   * @throws IllegalArgumentException if there is no matching tracer
   */
  def get[T <: Tracer](system: ActorSystem, tracerClass: Class[T]): T =
    apply(system, tracerClass)
}

/**
 * Akka Trace SPI.
 *
 * '''Important: tracer implementations must be thread-safe and non-blocking.'''
 *
 * A context object is returned by some methods and will be propagated across threads.
 * In some implementations the context object will not be needed and can simply be null.
 * For remote messages the trace context needs to be serialized to bytes.
 *
 * A local message flow will have the following calls:
 *
 *  - `actorTold` when the message is sent with `!` or `tell`
 *  - `actorReceived` at the beginning of message processing
 *  - `actorCompleted` at the end of message processing
 *
 * A remote message flow will have the following calls:
 *
 *  - `actorTold` when the message is first sent to a remote actor with `!` or `tell`
 *  - `remoteMessageSent` when the message is being sent over the wire
 *  - `remoteMessageReceived` before the message is processed on the receiving node
 *  - `actorTold` when the message is delivered locally on the receiving node
 *  - `remoteMessageCompleted` at the end of remote message processing
 *  - `actorReceived` at the beginning of message processing on the receiving node
 *  - `actorCompleted` at the end of message processing
 */
abstract class Tracer {
  /**
   * Record actor system started - after system initialisation and start.
   *
   * @param system the [[akka.actor.ActorSystem]] that has started
   */
  def systemStarted(system: ActorSystem): Unit

  /**
   * Record actor system shutdown - on system termination callback.
   *
   * '''Any tracer cleanup and shutdown can also happen at this point.'''
   *
   * @param system the [[akka.actor.ActorSystem]] that has shutdown
   */
  def systemShutdown(system: ActorSystem): Unit

  /**
   * Record actor told - on message send with `!` or `tell`.
   *
   * Returns a context object which will be attached to the message send,
   * and passed on the corresponding call to [[Tracer#actorReceived]].
   *
   * @param actorRef the [[akka.actor.ActorRef]] being told the message
   * @param message the message object
   * @param sender the sender [[akka.actor.ActorRef]] (may be dead letters)
   * @return a context object (or `Tracer.emptyContext` / `null` if not used)
   */
  def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any

  /**
   * Record actor received - at the beginning of message processing.
   *
   * Passes the context object returned by the corresponding [[Tracer#actorTold]].
   *
   * @param actorRef the self [[akka.actor.ActorRef]] of the actor
   * @param message the message object
   * @param sender the sender [[akka.actor.Actor]] (may be dead letters)
   * @param context the context object attached from `actorTold`
   */
  def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit

  /**
   * Record actor completed - at the end of message processing.
   *
   * @param actorRef the self [[akka.actor.ActorRef]] of the actor
   * @param message the message object
   * @param sender the sender [[akka.actor.Actor]] (may be dead letters)
   */
  def actorCompleted(actorRef: ActorRef, message: Any, sender: ActorRef): Unit

  /**
   * Record remote message sent - when remote message is going over the wire.
   *
   * Returns the serialized trace context, if used, otherwise an empty byte string.
   * The serialized trace context will be passed to [[Tracer#remoteMessageReceived]].
   *
   * @param actorRef the recipient [[akka.actor.ActorRef]] of the remote message
   * @param message the message object
   * @param size the size in bytes of the serialized user message object
   * @param sender the sender [[akka.actor.ActorRef]] (may be dead letters)
   * @param context the context object attached from `actorTold` and to be serialized
   * @return an [[akka.util.ByteString]] for the serialized trace context (can be an empty ByteString)
   */
  def remoteMessageSent(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: Any): ByteString

  /**
   * Record remote message received - before the processing of the remote message.
   *
   * Note that when the remote message is processed a local actor send will be made,
   * with a call to `actorTold`. It is up to the tracer implementation to transfer
   * the trace context to this `actorTold` call, if needed. The `remoteMessageReceived`
   * and `remoteMessageCompleted` calls frame any local message sends related to this
   * remote message being processed.
   *
   * The serialized trace context from [[Tracer#remoteMessageSent]] is passed in.
   *
   * @param actorRef the recipient [[akka.actor.ActorRef]] of the remote message
   * @param message the (deserialized) message object
   * @param size the size in bytes of the serialized user message object
   * @param sender the sender [[akka.actor.ActorRef]] (may be dead letters)
   * @param context the serialized trace context from `remoteMessageSent`
   */
  def remoteMessageReceived(actorRef: ActorRef, message: Any, size: Int, sender: ActorRef, context: ByteString): Unit

  /**
   * Record remote message completed - at the end of remote message processing.
   *
   * This call allows the cleanup of any thread-local state set on [[Tracer#remoteMessageReceived]].
   *
   * @param actorRef the recipient [[akka.actor.ActorRef]] of the remote message
   * @param message the (deserialized) message object
   * @param size the size in bytes of the serialized user message object
   * @param sender the sender [[akka.actor.ActorRef]] (may be dead letters)
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
