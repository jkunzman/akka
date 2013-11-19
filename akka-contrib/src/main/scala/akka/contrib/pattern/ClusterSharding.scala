/**
 *  Copyright (C) 2009-2013 Typesafe Inc. <http://www.typesafe.com>
 */
package akka.contrib.pattern

import java.net.URLEncoder
import java.util.concurrent.ConcurrentHashMap
import scala.collection.immutable
import scala.concurrent.Await
import scala.concurrent.duration._
import scala.concurrent.forkjoin.ThreadLocalRandom
import akka.actor.Actor
import akka.actor.ActorLogging
import akka.actor.ActorRef
import akka.actor.ActorSelection
import akka.actor.ActorSystem
import akka.actor.ExtendedActorSystem
import akka.actor.Extension
import akka.actor.ExtensionId
import akka.actor.ExtensionIdProvider
import akka.actor.NoSerializationVerificationNeeded
import akka.actor.PoisonPill
import akka.actor.Props
import akka.actor.ReceiveTimeout
import akka.actor.RootActorPath
import akka.actor.Terminated
import akka.cluster.Cluster
import akka.cluster.ClusterEvent.CurrentClusterState
import akka.cluster.ClusterEvent.MemberEvent
import akka.cluster.ClusterEvent.MemberRemoved
import akka.cluster.ClusterEvent.MemberUp
import akka.cluster.Member
import akka.cluster.MemberStatus
import akka.pattern.ask
import akka.persistence.EventsourcedProcessor

object ClusterSharding extends ExtensionId[ClusterSharding] with ExtensionIdProvider {
  override def get(system: ActorSystem): ClusterSharding = super.get(system)

  override def lookup = ClusterSharding

  override def createExtension(system: ExtendedActorSystem): ClusterSharding =
    new ClusterSharding(system)
}

class ClusterSharding(system: ExtendedActorSystem) extends Extension {
  import ClusterShardingGuardian._
  import ShardCoordinator.ShardAllocationStrategy
  import ShardCoordinator.LeastShardAllocationStrategy

  private val cluster = Cluster(system)
  /**
   * INTERNAL API
   */
  private[akka] object Settings {
    val config = system.settings.config.getConfig("akka.contrib.cluster.sharding")
    val Role: Option[String] = config.getString("role") match {
      case "" ⇒ None
      case r  ⇒ Some(r)
    }
    val HasClusterSelfRoles: Boolean = Role.forall(cluster.selfRoles.contains)
    val GuardianName: String = config.getString("guardian-name")
    val RetryInterval: FiniteDuration = Duration(config.getMilliseconds("retry-interval"), MILLISECONDS)
    val BufferSize: Int = config.getInt("buffer-size")
    val HandOffTimeout: FiniteDuration = Duration(config.getMilliseconds("handoff-timeout"), MILLISECONDS)
    val RebalanceInterval: FiniteDuration = Duration(config.getMilliseconds("rebalance-interval"), MILLISECONDS)
    val LeastShardAllocationRebalanceThreshold: Int =
      config.getInt("least-shard-allocation-strategy.rebalance-threshold")
    val LeastShardAllocationMaxSimultaneousRebalance: Int =
      config.getInt("least-shard-allocation-strategy.max-simultaneous-rebalance")
  }
  import Settings._
  private val regions: ConcurrentHashMap[String, ActorRef] = new ConcurrentHashMap
  private lazy val guardian = system.actorOf(Props[ClusterShardingGuardian], Settings.GuardianName)

  def start(
    typeName: String,
    entryProps: Option[Props],
    idExtractor: ShardRegion.IdExtractor,
    shardResolver: ShardRegion.ShardResolver,
    allocationStrategy: ShardAllocationStrategy): Unit = {
    implicit val timeout = system.settings.CreationTimeout
    val startMsg = Start(typeName, entryProps, idExtractor, shardResolver, allocationStrategy)
    val Started(shardRegion) = Await.result(guardian ? startMsg, timeout.duration)
    regions.put(typeName, shardRegion)
  }

  def start(
    typeName: String,
    entryProps: Option[Props],
    idExtractor: ShardRegion.IdExtractor,
    shardResolver: ShardRegion.ShardResolver): Unit =
    start(typeName, entryProps, idExtractor, shardResolver,
      new LeastShardAllocationStrategy(LeastShardAllocationRebalanceThreshold, LeastShardAllocationMaxSimultaneousRebalance))

  /**
   * Java API
   */
  def start(
    typeName: String,
    entryProps: Props,
    messageExtractor: ShardRegion.MessageExtractor,
    allocationStrategy: ShardAllocationStrategy): Unit =
    start(typeName, entryProps = Option(entryProps),
      idExtractor = {
        case msg if messageExtractor.entryId(msg) ne null ⇒
          (messageExtractor.entryId(msg), messageExtractor.entryMessage(msg))
      },
      shardResolver = msg ⇒ messageExtractor.shardId(msg),
      allocationStrategy = allocationStrategy)

  /**
   * Java API
   */
  def start(
    typeName: String,
    entryProps: Props,
    messageExtractor: ShardRegion.MessageExtractor): Unit =
    start(typeName, entryProps, messageExtractor,
      new LeastShardAllocationStrategy(LeastShardAllocationRebalanceThreshold, LeastShardAllocationMaxSimultaneousRebalance))

  def shardRegion(typeName: String): ActorRef = regions.get(typeName) match {
    case null ⇒ throw new IllegalArgumentException(s"Shard type [$typeName] must be started first")
    case ref  ⇒ ref
  }

}

object ClusterShardingGuardian {
  import ShardCoordinator.ShardAllocationStrategy
  case class Start(typeName: String, entryProps: Option[Props], idExtractor: ShardRegion.IdExtractor,
                   shardResolver: ShardRegion.ShardResolver, allocationStrategy: ShardAllocationStrategy)
    extends NoSerializationVerificationNeeded
  case class Started(shardRegion: ActorRef) extends NoSerializationVerificationNeeded
}

class ClusterShardingGuardian extends Actor {
  import ClusterShardingGuardian._

  val cluster = Cluster(context.system)
  val sharding = ClusterSharding(context.system)
  import sharding.Settings._

  def receive = {
    case Start(typeName, entryProps, idExtractor, shardResolver, allocationStrategy) ⇒
      val encName = URLEncoder.encode(typeName, "utf-8")
      val coordinatorSingletonManagerName = encName + "Coordinator"
      val coordinatorPath = (self.path / coordinatorSingletonManagerName / "singleton").elements.mkString("/", "/", "")
      val shardRegion = context.child(encName).getOrElse {
        if (HasClusterSelfRoles && context.child(coordinatorSingletonManagerName).isEmpty) {
          context.actorOf(ClusterSingletonManager.props(
            singletonProps = ShardCoordinator.props(handOffTimeout = HandOffTimeout, rebalanceInterval = RebalanceInterval, allocationStrategy),
            singletonName = "singleton",
            terminationMessage = PoisonPill,
            role = Role),
            name = coordinatorSingletonManagerName)
        }
        if (entryProps.isDefined && HasClusterSelfRoles)
          context.actorOf(ShardRegion.props(
            entryProps.get,
            role = Role,
            coordinatorPath = coordinatorPath,
            retryInterval = RetryInterval,
            bufferSize = BufferSize,
            idExtractor = idExtractor,
            shardResolver = shardResolver),
            name = encName)
        else
          context.actorOf(ShardRegion.proxyProps(
            role = Role,
            coordinatorPath = coordinatorPath,
            retryInterval = RetryInterval,
            bufferSize = BufferSize,
            idExtractor = idExtractor,
            shardResolver = shardResolver),
            name = encName)
      }
      sender ! Started(shardRegion)

  }

}

object ShardRegion {

  /**
   * Scala API
   */
  def props(
    entryProps: Props,
    role: Option[String],
    coordinatorPath: String,
    retryInterval: FiniteDuration,
    bufferSize: Int,
    idExtractor: ShardRegion.IdExtractor,
    shardResolver: ShardRegion.ShardResolver): Props =
    Props(classOf[ShardRegion], Some(entryProps), role, coordinatorPath, retryInterval, bufferSize, idExtractor, shardResolver)

  /**
   * Java API
   */
  def props(
    entryProps: Props,
    role: String,
    coordinatorPath: String,
    retryInterval: FiniteDuration,
    bufferSize: Int,
    messageExtractor: ShardRegion.MessageExtractor): Props =
    props(entryProps, roleOption(role), coordinatorPath, retryInterval, bufferSize,
      idExtractor = {
        case msg if messageExtractor.entryId(msg) ne null ⇒
          (messageExtractor.entryId(msg), messageExtractor.entryMessage(msg))
      },
      shardResolver = msg ⇒ messageExtractor.shardId(msg))

  /**
   * Scala API
   */
  def proxyProps(
    role: Option[String],
    coordinatorPath: String,
    retryInterval: FiniteDuration,
    bufferSize: Int,
    idExtractor: ShardRegion.IdExtractor,
    shardResolver: ShardRegion.ShardResolver): Props =
    Props(classOf[ShardRegion], None, role, coordinatorPath, retryInterval, bufferSize, idExtractor, shardResolver)

  /**
   * Java API
   */
  def proxyProps(
    role: String,
    coordinatorPath: String,
    retryInterval: FiniteDuration,
    bufferSize: Int,
    messageExtractor: ShardRegion.MessageExtractor): Props =
    proxyProps(roleOption(role), coordinatorPath, retryInterval, bufferSize,
      idExtractor = {
        case msg if messageExtractor.entryId(msg) ne null ⇒
          (messageExtractor.entryId(msg), messageExtractor.entryMessage(msg))
      },
      shardResolver = msg ⇒ messageExtractor.shardId(msg))

  type EntryId = String
  type ShardId = String
  type Msg = Any
  type IdExtractor = PartialFunction[Msg, (EntryId, Msg)]
  type ShardResolver = Msg ⇒ ShardId

  /**
   * Java API
   */
  trait MessageExtractor {
    def entryId(message: Any): String
    def entryMessage(message: Any): Any
    def shardId(message: Any): String
  }

  @SerialVersionUID(1L) case class Passivate(stopMessage: Any = PoisonPill)

  private case object Retry

  private def roleOption(role: String): Option[String] = role match {
    case null | "" ⇒ None
    case _         ⇒ Some(role)
  }

  /**
   * INTERNAL API
   */
  private[akka] class HandOffStopper(shard: String, replyTo: ActorRef, entries: Set[ActorRef]) extends Actor {
    import ShardCoordinator.Internal.ShardStopped

    entries.foreach { a ⇒
      context watch a
      a ! PoisonPill
    }

    var remaining = entries

    def receive = {
      case Terminated(ref) ⇒
        remaining -= ref
        if (remaining.isEmpty) {
          replyTo ! ShardStopped(shard)
          context stop self
        }
    }
  }
}

class ShardRegion(
  entryProps: Option[Props],
  role: Option[String],
  coordinatorPath: String,
  retryInterval: FiniteDuration,
  bufferSize: Int,
  idExtractor: ShardRegion.IdExtractor,
  shardResolver: ShardRegion.ShardResolver) extends Actor with ActorLogging {

  import ShardCoordinator.Internal._
  import ShardRegion._

  val cluster = Cluster(context.system)

  // sort by age, oldest first
  val ageOrdering = Ordering.fromLessThan[Member] { (a, b) ⇒ a.isOlderThan(b) }
  var membersByAge: immutable.SortedSet[Member] = immutable.SortedSet.empty(ageOrdering)

  var regions = Map.empty[ActorRef, ShardId]
  var regionByShard = Map.empty[ShardId, ActorRef]
  var entries = Map.empty[ActorRef, ShardId]
  var entriesByShard = Map.empty[ShardId, Set[ActorRef]]
  var shardBuffers = Map.empty[ShardId, Vector[(Msg, ActorRef)]]
  var passivatingBuffers = Map.empty[ActorRef, Vector[(Msg, ActorRef)]]

  def totalBufferSize = {
    shardBuffers.map { case (_, buf) ⇒ buf.size }.sum +
      passivatingBuffers.map { case (_, buf) ⇒ buf.size }.sum
  }

  import context.dispatcher
  val retryTask = context.system.scheduler.schedule(retryInterval, retryInterval, self, Retry)

  // subscribe to MemberEvent, re-subscribe when restart
  override def preStart(): Unit = {
    cluster.subscribe(self, classOf[MemberEvent])
  }

  override def postStop(): Unit = {
    super.postStop()
    cluster.unsubscribe(self)
    retryTask.cancel()
  }

  def matchingRole(member: Member): Boolean = role match {
    case None    ⇒ true
    case Some(r) ⇒ member.hasRole(r)
  }

  def coordinatorSelection: Option[ActorSelection] =
    membersByAge.headOption.map(m ⇒ context.actorSelection(RootActorPath(m.address) + coordinatorPath))

  var coordinator: Option[ActorRef] = None

  def changeMembers(newMembers: immutable.SortedSet[Member]): Unit = {
    val before = membersByAge.headOption
    val after = newMembers.headOption
    membersByAge = newMembers
    if (before != after) {
      // FIXME change all log.info to log.debug
      log.info("Coordinator located at [{}]", after.map(_.address))
      coordinator = None
      register()
    }
  }

  def receive = {
    case state: CurrentClusterState ⇒
      changeMembers {
        immutable.SortedSet.empty(ageOrdering) ++ state.members.collect {
          case m if m.status == MemberStatus.Up && matchingRole(m) ⇒ m
        }
      }

    case MemberUp(m) ⇒
      if (matchingRole(m))
        changeMembers(membersByAge + m)

    case MemberRemoved(m, _) ⇒
      if (matchingRole(m))
        changeMembers(membersByAge - m)

    case ShardHome(shard, ref) ⇒
      log.info("Shard [{}] located at [{}]", shard, ref)
      regionByShard.get(shard) match {
        case Some(r) if r == context.self && ref != context.self ⇒
          throw new IllegalStateException(s"Unannounced change of shard [${shard}] from self to [${ref}]")
        case _ ⇒
      }
      regionByShard = regionByShard.updated(shard, ref)
      regions = regions.updated(ref, shard)
      if (ref != context.self)
        context.watch(ref)
      shardBuffers.get(shard) match {
        case Some(buf) ⇒
          buf.foreach {
            case (msg, snd) ⇒ deliverMessage(msg, snd)
          }
          shardBuffers -= shard
        case None ⇒
      }

    case Terminated(ref) ⇒
      if (coordinator.exists(_ == ref))
        coordinator = None
      else if (regions.contains(ref)) {
        val shard = regions(ref)
        regionByShard -= shard
        regions -= ref
      } else if (entries.contains(ref)) {
        val shard = entries(ref)
        val newShardEntities = entriesByShard(shard) - ref
        if (newShardEntities.isEmpty)
          entriesByShard -= shard
        else
          entriesByShard = entriesByShard.updated(shard, newShardEntities)
        entries -= ref
        if (passivatingBuffers.contains(ref)) {
          log.info("Passivating completed {}, buffered [{}]", ref, passivatingBuffers(ref).size)
          // deliver messages that were received between Passivate and Terminated, 
          // will create new entry instance and deliver the messages to it
          passivatingBuffers(ref) foreach {
            case (msg, snd) ⇒ deliverMessage(msg, snd)
          }
          passivatingBuffers -= ref
        }
      }

    case RegisterAck(coord) ⇒
      context.watch(coord)
      coordinator = Some(coord)
      requestShardBufferHomes()

    case Retry ⇒
      if (coordinator.isEmpty)
        register()
      else
        requestShardBufferHomes()

    case BeginHandOff(shard) ⇒
      log.info("BeginHandOff shard [{}]", shard)
      if (regionByShard.contains(shard)) {
        regions -= regionByShard(shard)
        regionByShard -= shard
      }
      sender ! BeginHandOffAck(shard)

    case HandOff(shard) ⇒
      log.info("HandOff shard [{}]", shard)

      // must drop requests that came in between the BeginHandOff and now,
      // because they might be forwarded from other regions and there
      // is a risk or message re-ordering otherwise
      // FIXME do we need to be even more strict?
      if (shardBuffers.contains(shard))
        shardBuffers -= shard

      if (entriesByShard.contains(shard))
        context.actorOf(Props(classOf[HandOffStopper], shard, sender, entriesByShard(shard)))
      else
        sender ! ShardStopped(shard)

    case Passivate(stopMessage) ⇒
      passivate(sender, stopMessage)
    case Passivate ⇒
      passivate(sender, PoisonPill)

    case msg if idExtractor.isDefinedAt(msg) ⇒
      deliverMessage(msg, sender)

  }

  def register(): Unit = {
    coordinatorSelection.foreach(_ ! registrationMessage)
  }

  def registrationMessage: Any =
    if (entryProps.isDefined) Register(self) else RegisterProxy(self)

  def requestShardBufferHomes(): Unit = {
    shardBuffers.foreach {
      case (shard, _) ⇒ coordinator.foreach { c ⇒
        log.info("Retry request for shard [{}] homes", shard)
        c ! GetShardHome(shard)
      }
    }
  }

  def deliverMessage(msg: Any, snd: ActorRef): Unit = {
    val shard = shardResolver(msg)
    regionByShard.get(shard) match {
      case Some(ref) if ref == context.self ⇒
        val (id, m) = idExtractor(msg)
        val name = URLEncoder.encode(id, "utf-8")
        val entry = context.child(name).getOrElse {
          if (entryProps.isEmpty)
            throw new IllegalStateException("Shard must not be allocated to a proxy only ShardRegion")
          log.info("Starting entry [{}] in shard [{}]", id, shard)
          val a = context.watch(context.actorOf(entryProps.get, name))
          entries = entries.updated(a, shard)
          entriesByShard = entriesByShard.updated(shard, entriesByShard.getOrElse(shard, Set.empty) + a)
          a
        }
        passivatingBuffers.get(entry) match {
          case None ⇒
            log.info("Message [{}] for shard [{}] sent to entry", m.getClass.getName, shard)
            entry.tell(m, snd)
          case Some(buf) ⇒
            if (totalBufferSize >= bufferSize) {
              log.info("Buffer is full, dropping message for passivated entry in shard [{}]", shard)
              context.system.deadLetters ! msg
            } else {
              log.info("Message for shard [{}] buffered due to entry being passivated", shard)
              passivatingBuffers = passivatingBuffers.updated(entry, buf :+ ((msg, snd)))
            }
        }
      case Some(ref) ⇒
        log.info("Forwarding request for shard [{}] to [{}]", shard, ref)
        ref.tell(msg, snd)
      case None ⇒
        if (!shardBuffers.contains(shard)) {
          log.info("Request shard [{}] home", shard)
          coordinator.foreach(_ ! GetShardHome(shard))
        }
        if (totalBufferSize >= bufferSize) {
          log.info("Buffer is full, dropping message for shard [{}]", shard)
          context.system.deadLetters ! msg
        } else {
          val buf = shardBuffers.getOrElse(shard, Vector.empty)
          shardBuffers = shardBuffers.updated(shard, buf :+ ((msg, snd)))
        }
    }
  }

  def passivate(entry: ActorRef, stopMessage: Any): Unit = {
    val entry = sender
    if (entries.contains(entry) && !passivatingBuffers.contains(entry)) {
      log.info("Passivating started {}", entry)
      passivatingBuffers = passivatingBuffers.updated(entry, Vector.empty)
      entry ! stopMessage
    }
  }

}

object ShardCoordinator {

  import ShardRegion.ShardId

  def props(handOffTimeout: FiniteDuration, rebalanceInterval: FiniteDuration,
            allocationStrategy: ShardAllocationStrategy): Props =
    Props(classOf[ShardCoordinator], handOffTimeout, rebalanceInterval, allocationStrategy)

  trait ShardAllocationStrategy extends NoSerializationVerificationNeeded {
    def allocateShard(requester: ActorRef, shardId: ShardId,
                      currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): ActorRef
    def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                  rebalanceInProgress: Set[ShardId]): Option[ShardId]
  }

  /**
   * Java API
   */
  abstract class AbstractShardAllocationStrategy extends ShardAllocationStrategy {
    override final def allocateShard(requester: ActorRef, shardId: ShardId,
                                     currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): ActorRef = {
      import scala.collection.JavaConverters._
      allocateShard(requester, shardId, currentShardAllocations.asJava)
    }

    override final def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                                 rebalanceInProgress: Set[ShardId]): Option[ShardId] = {
      import scala.collection.JavaConverters._
      Option(rebalance(currentShardAllocations.asJava, rebalanceInProgress.asJava))
    }

    def allocateShard(requester: ActorRef, shardId: String,
                      currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]]): ActorRef
    def rebalance(currentShardAllocations: java.util.Map[ActorRef, immutable.IndexedSeq[String]],
                  rebalanceInProgress: java.util.Set[String]): String
  }

  class LeastShardAllocationStrategy(rebalanceThreshold: Int, maxSimultaneousRebalance: Int) extends ShardAllocationStrategy {
    override def allocateShard(requester: ActorRef, shardId: ShardId,
                               currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): ActorRef = {
      val (regionWithLeastShards, _) = currentShardAllocations.minBy { case (_, v) ⇒ v.size }
      regionWithLeastShards
    }

    override def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                           rebalanceInProgress: Set[ShardId]): Option[ShardId] = {
      if (rebalanceInProgress.size < maxSimultaneousRebalance) {
        val (regionWithLeastShards, leastShards) = currentShardAllocations.minBy { case (_, v) ⇒ v.size }
        val mostShards = currentShardAllocations.collect {
          case (_, v) ⇒ v.filterNot(s ⇒ rebalanceInProgress(s))
        }.maxBy(_.size)
        if (mostShards.size - leastShards.size >= rebalanceThreshold)
          Some(mostShards.last)
        else
          None
      } else None
    }
  }

  class FirstRequesterShardAllocationStrategy extends ShardAllocationStrategy {
    override def allocateShard(requester: ActorRef, shardId: ShardId,
                               currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]]): ActorRef = {
      if (currentShardAllocations.contains(requester))
        requester
      else {
        val all = currentShardAllocations.keys.toVector
        all(ThreadLocalRandom.current.nextInt(all.size))
      }
    }

    override def rebalance(currentShardAllocations: Map[ActorRef, immutable.IndexedSeq[ShardId]],
                           rebalanceInProgress: Set[ShardId]): Option[ShardId] = None
  }

  /**
   * INTERNAL API
   */
  private[akka] object Internal {
    @SerialVersionUID(1L) case class Register(shardRegion: ActorRef)
    @SerialVersionUID(1L) case class RegisterProxy(shardRegionProxy: ActorRef)
    @SerialVersionUID(1L) case class RegisterAck(coordinator: ActorRef)
    @SerialVersionUID(1L) case class GetShardHome(shard: ShardId)
    @SerialVersionUID(1L) case class ShardHome(shard: ShardId, ref: ActorRef)
    @SerialVersionUID(1L) private[akka] case class BeginHandOff(shard: ShardId)
    @SerialVersionUID(1L) private[akka] case class BeginHandOffAck(shard: ShardId)
    @SerialVersionUID(1L) private[akka] case class HandOff(shard: ShardId)
    @SerialVersionUID(1L) private[akka] case class ShardStopped(shard: ShardId)

    // DomainEvents
    sealed trait DomainEvent
    @SerialVersionUID(1L) case class ShardRegionRegistered(region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) case class ShardRegionProxyRegistered(regionProxy: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) case class ShardRegionTerminated(region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) case class ShardRegionProxyTerminated(regionProxy: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) case class ShardHomeAllocated(shard: ShardId, region: ActorRef) extends DomainEvent
    @SerialVersionUID(1L) case class ShardHomeDeallocated(shard: ShardId) extends DomainEvent

    object State {
      val empty = State()
    }

    @SerialVersionUID(1L) case class State private (
      // region for each shard   
      val shards: Map[ShardId, ActorRef] = Map.empty,
      // shards for each region
      val regions: Map[ActorRef, Vector[ShardId]] = Map.empty,
      val regionProxies: Set[ActorRef] = Set.empty) {

      def updated(event: DomainEvent): State = event match {
        case ShardRegionRegistered(region) ⇒
          copy(regions = regions.updated(region, Vector.empty))
        case ShardRegionProxyRegistered(proxy) ⇒
          copy(regionProxies = regionProxies + proxy)
        case ShardRegionTerminated(region) ⇒
          copy(
            regions = regions - region,
            shards = shards -- regions(region))
        case ShardRegionProxyTerminated(proxy) ⇒
          copy(regionProxies = regionProxies - proxy)
        case ShardHomeAllocated(shard, region) ⇒
          copy(
            shards = shards.updated(shard, region),
            regions = regions.updated(region, regions(region) :+ shard))
        case ShardHomeDeallocated(shard) ⇒
          val region = shards(shard)
          copy(
            shards = shards - shard,
            regions = regions.updated(region, regions(region).filterNot(_ == shard)))
      }
    }

  }

  private case object Rebalance
  private case class RebalanceDone(shard: ShardId, ok: Boolean)

  /**
   * INTERNAL API
   */
  private[akka] class RebalanceWorker(shard: String, from: ActorRef, handOffTimeout: FiniteDuration,
                                      regions: Set[ActorRef]) extends Actor {
    import Internal._
    regions.foreach(_ ! BeginHandOff(shard))
    var remaining = regions

    import context.dispatcher
    context.system.scheduler.scheduleOnce(handOffTimeout, self, ReceiveTimeout)

    def receive = {
      case BeginHandOffAck(`shard`) ⇒
        remaining -= sender
        if (remaining.isEmpty) {
          from ! HandOff(shard)
          context.become(stoppingShard, discardOld = true)
        }
      case ReceiveTimeout ⇒ done(ok = false)
    }

    def stoppingShard: Receive = {
      case ShardStopped(shard) ⇒ done(ok = true)
      case ReceiveTimeout      ⇒ done(ok = false)
    }

    def done(ok: Boolean): Unit = {
      context.parent ! RebalanceDone(shard, ok)
      context.stop(self)
    }
  }

}

class ShardCoordinator(handOffTimeout: FiniteDuration, rebalanceInterval: FiniteDuration,
                       allocationStrategy: ShardCoordinator.ShardAllocationStrategy)
  extends EventsourcedProcessor with ActorLogging {
  import ShardCoordinator._
  import ShardCoordinator.Internal._
  import ShardRegion.ShardId

  override def processorId = self.path.elements.mkString("/", "/", "")

  var persistentState = State.empty
  var rebalanceInProgress = Set.empty[ShardId]

  import context.dispatcher
  val rebalanceTask = context.system.scheduler.schedule(rebalanceInterval, rebalanceInterval, self, Rebalance)

  override def postStop(): Unit = {
    super.postStop()
    rebalanceTask.cancel()
  }

  // FIXME add persistence snapshotting of State

  override def receiveReplay: Receive = {
    case evt: DomainEvent ⇒ evt match {
      case ShardRegionRegistered(region) ⇒
        context.watch(region)
        persistentState = persistentState.updated(evt)
      case ShardRegionProxyRegistered(proxy) ⇒
        context.watch(proxy)
        persistentState = persistentState.updated(evt)
      case ShardRegionTerminated(region) ⇒
        context.unwatch(region)
        persistentState = persistentState.updated(evt)
      case ShardRegionProxyTerminated(proxy) ⇒
        context.unwatch(proxy)
        persistentState = persistentState.updated(evt)
      case _: ShardHomeAllocated ⇒
        persistentState = persistentState.updated(evt)
      case _: ShardHomeDeallocated ⇒
        persistentState = persistentState.updated(evt)
    }
  }

  override def receiveCommand: Receive = {
    case Register(region) ⇒
      log.info("ShardRegion registered: [{}]", region)
      if (persistentState.regions.contains(region))
        sender ! RegisterAck(self)
      else
        persist(ShardRegionRegistered(region)) { evt ⇒
          persistentState = persistentState.updated(evt)
          context.watch(region)
          sender ! RegisterAck(self)
        }

    case RegisterProxy(proxy) ⇒
      log.info("ShardRegion proxy registered: [{}]", proxy)
      if (persistentState.regionProxies.contains(proxy))
        sender ! RegisterAck(self)
      else
        persist(ShardRegionProxyRegistered(proxy)) { evt ⇒
          persistentState = persistentState.updated(evt)
          context.watch(proxy)
          sender ! RegisterAck(self)
        }

    case Terminated(ref) ⇒
      if (persistentState.regions.contains(ref)) {
        log.info("ShardRegion terminated: [{}]", ref)
        persist(ShardRegionTerminated(ref)) { evt ⇒
          persistentState = persistentState.updated(evt)
        }
      } else if (persistentState.regionProxies.contains(ref)) {
        log.info("ShardRegion proxy terminated: [{}]", ref)
        persist(ShardRegionProxyTerminated(ref)) { evt ⇒
          persistentState = persistentState.updated(evt)
        }
      }

    case GetShardHome(shard) ⇒
      if (!rebalanceInProgress.contains(shard)) {
        persistentState.shards.get(shard) match {
          case Some(ref) ⇒ sender ! ShardHome(shard, ref)
          case None ⇒
            if (persistentState.regions.nonEmpty) {
              val region = allocationStrategy.allocateShard(sender, shard, persistentState.regions)
              persist(ShardHomeAllocated(shard, region)) { evt ⇒
                persistentState = persistentState.updated(evt)
                log.info("Shard [{}] allocated at [{}]", evt.shard, evt.region)
                sender ! ShardHome(evt.shard, evt.region)
              }
            }
        }
      }

    case Rebalance ⇒
      allocationStrategy.rebalance(persistentState.regions, rebalanceInProgress).foreach { shard ⇒
        rebalanceInProgress += shard
        val rebalanceFromRegion = persistentState.shards(shard)
        log.info("Rebalance shard [{}] from [{}]", shard, rebalanceFromRegion)
        context.actorOf(Props(classOf[RebalanceWorker], shard, rebalanceFromRegion, handOffTimeout,
          persistentState.regions.keySet ++ persistentState.regionProxies))
      }

    case RebalanceDone(shard, ok) ⇒
      rebalanceInProgress -= shard
      log.info("Rebalance shard [{}] done [{}]", shard, ok)
      if (ok) persist(ShardHomeDeallocated(shard)) { evt ⇒
        persistentState = persistentState.updated(evt)
        log.info("Shard [{}] deallocated", evt.shard)
      }

  }

}

