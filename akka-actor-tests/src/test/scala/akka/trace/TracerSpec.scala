/**
 * Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */

package akka.trace

import akka.actor._
import akka.testkit._
import akka.util.ByteString
import com.typesafe.config.{ Config, ConfigFactory }
import scala.concurrent.duration._

object TracerSpec {
  val testConfig: Config = ConfigFactory.parseString("""
    # print tracer can be added for debugging: "akka.trace.PrintTracer"
    akka.tracers = ["akka.trace.TracerSpec$MessageTracer", "akka.trace.CountTracer"]
  """)

  /**
   * Tracer that checks the message context passing between send and receive.
   */
  class MessageTracer(config: Config) extends EmptyTracer {
    case class MessageContext(path: String, message: String)

    def checkMessageContext(context: Any, actorRef: ActorRef, message: Any): Unit = {
      val messageContext = context match {
        case mc: MessageContext ⇒ mc
        case _                  ⇒ sys.error("No message context attached")
      }
      assert(messageContext.path == actorRef.path.toString)
      assert(messageContext.message == message.toString)
    }

    override def actorTold(actorRef: ActorRef, message: Any, sender: ActorRef): Any =
      MessageContext(actorRef.path.toString, message.toString)

    override def actorReceived(actorRef: ActorRef, message: Any, sender: ActorRef, context: Any): Unit =
      checkMessageContext(context, actorRef, message)
  }
}

class TracerSpec extends AkkaSpec(TracerSpec.testConfig) with ImplicitSender with DefaultTimeout {
  val tracer = CountTracer(system)

  "Actor tracing" must {

    "trace actor system start" in {
      tracer.counts.systemStarted.get must be(1)
    }

    "trace actor messages" in {
      tracer.counts.reset()
      val actor = system.actorOf(Props(new Actor {
        def receive = { case message ⇒ sender ! message }
      }))
      actor ! "message"
      expectMsg("message")
      tracer.counts.actorTold.get must be(2)
      tracer.counts.actorReceived.get must be(2)
      // actor completed for test actor may not have been recorded yet
      tracer.counts.actorCompleted.get must be >= 1L
    }

    "trace actor system shutdown" in {
      system.shutdown()
      system.awaitTermination(timeout.duration)
      tracer.counts.systemShutdown.get must be(1)
    }
  }
}
