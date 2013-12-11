/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.remote.serialization

import akka.actor.ExtendedActorSystem
import akka.remote.MessageSerializer
import akka.remote.WireFormats.TraceEnvelope
import akka.serialization.Serializer
import akka.trace.TracedMessage
import akka.util.ByteString
import com.google.protobuf.{ ByteString ⇒ ProtobufByteString }

/**
 * Serialize traced message (wrapper that adds trace context to a message).
 */
class TracedMessageSerializer(system: ExtendedActorSystem) extends Serializer {

  def identifier: Int = 10

  def includeManifest: Boolean = false

  def toBinary(obj: AnyRef): Array[Byte] = obj match {
    case traced: TracedMessage ⇒ serializeTracedMessage(traced)
    case _                     ⇒ throw new IllegalArgumentException(s"Expected to serialize TracedMessage but got ${obj.getClass.getName}")
  }

  def fromBinary(bytes: Array[Byte], manifest: Option[Class[_]]): AnyRef = {
    val traced = deserializeTracedMessage(bytes)
    if (system.hasTracer) traced else traced.messageRef
  }

  private def serializeTracedMessage(traced: TracedMessage): Array[Byte] = {
    val builder = TraceEnvelope.newBuilder
    val contextBytes = system.tracer.serializeContext(traced.context)
    builder.setMessage(MessageSerializer.serialize(system, traced.messageRef))
    builder.setContext(ProtobufByteString.copyFrom(contextBytes.asByteBuffer))
    builder.build.toByteArray
  }

  private def deserializeTracedMessage(bytes: Array[Byte]): TracedMessage = {
    val traceEnvelope = TraceEnvelope.parseFrom(bytes)
    val message = MessageSerializer.deserialize(system, traceEnvelope.getMessage)
    val context = system.tracer.deserializeContext(ByteString(traceEnvelope.getContext.asReadOnlyByteBuffer))
    TracedMessage(message, context)
  }
}
