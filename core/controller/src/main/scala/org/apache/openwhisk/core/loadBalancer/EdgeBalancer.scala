package org.apache.openwhisk.core.loadBalancer

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector.{ActivationMessage, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.entity._
import org.apache.openwhisk.core.entity.size._
import org.apache.openwhisk.spi.SpiLoader

import scala.concurrent.Future
import scala.util.Random

/**
 * ## Limitations:
 * - We assume a single controller (loadbalancer) rather than a coordinated cluster
 * - We currently ignores managedFraction and blackboxFraction, i.e., we don't distinguish between managed functions and
 *   blackbox functions
 *
 * @param config
 * @param controllerInstance
 * @param feedFactory
 * @param invokerPoolFactory
 * @param messagingProvider
 * @param actorSystem
 * @param logging
 * @param materializer
 */
class EdgeBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  val invokerPoolFactory: InvokerPoolFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  logging.info(this, s"Initializing $getClass")

  override protected def emitMetrics(): Unit = {
    super.emitMetrics()
    // TODO: Add other metrics
  }

  private implicit val random: Random = new Random()

  private val monitor = actorSystem.actorOf(Props(new Actor{
    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) => EdgeBalancerStateSingleton.updateInvokers(newState)

      // case CurrentContainerState(newState) => schedulingState.updateContainers(newState)
    }
  }))

  override val invokerPool: ActorRef = invokerPoolFactory.createInvokerPool(
    actorSystem,
    messagingProvider,
    messageProducer,
    sendActivationToInvoker,
    Some(monitor)
  )

  override def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = {

  }

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(EdgeBalancerStateSingleton.invokers)

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)
                      (implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    logging.info(this, s"User: ${msg.user.namespace.name.asString} with weight ${msg.user.limits.weight}")
    logging.info(this, s"Action: ${action.name.asString} with weight: ${action.limits.weight}")

    EdgeBalancer.schedule(msg.user.namespace, msg.action, EdgeBalancerStateSingleton).map{ containerInfo =>
      val invoker = containerInfo.invoker
      val msgWithContainerId = msg.copy(containerId = Some(containerInfo.id), size=Some((containerInfo.memory, containerInfo.cpu)))
      val activationResult = setupActivation(msgWithContainerId, action, invoker)
      sendActivationToInvoker(messageProducer, msgWithContainerId, invoker).map(_ => activationResult)
    }.getOrElse {
      // TODO: This should be due to the action has no running containers.
      // Create one container and schedule to it.
      val invoker = EdgeBalancerStateSingleton.invokers.head.id
      val containerId = UUID().asString
      val memory = action.limits.memory.megabytes.MB
      val cpu = action.limits.cpu.cpuTime

      val coldStartMsg = msg.copy(containerId = Some(containerId), size = Some((memory, cpu)))
      val activationResult = setupActivation(coldStartMsg, action, invoker)
      sendActivationToInvoker(messageProducer, coldStartMsg, invoker)map { _ =>
        addContainer(msg.user.namespace, msg.action, ContainerInfo(containerId, invoker, memory, cpu))
        activationResult
      }
    }
  }

  def addContainer(user: Namespace, action: FullyQualifiedEntityName, containerInfo: ContainerInfo): Future[Unit] = {
    Future {
      var updated = false
      while (!updated) {
        EdgeBalancerStateSingleton.getContainers(user, action) match {
          case Some(oldContainers) =>
            updated = EdgeBalancerStateSingleton.compareAndSetContainers(user, action, oldContainers, oldContainers :+ containerInfo)
          case None =>
            updated = EdgeBalancerStateSingleton.putIfAbsent(user, action, IndexedSeq(containerInfo))
        }
      }
      logging.info(this, s"Added container with ${containerInfo.memory} memory and ${containerInfo.cpu} cpu for action $user/$action")
    }
  }

//  private def sendControlMessageToInvoker(producer: MessageProducer, msg: ContainerOperationMessage,  invoker: InvokerInstanceId) = {
//    implicit val transactionId = msg.transid
//
//    val topic = s"invoker${invoker.toInt}-control"
//
//    val start = transactionId.started(
//      this,
//      LoggingMarkers.CONTROLLER_KAFKA,
//      s"posting topic '$topic' with ${msg}'",
//      logLevel = Logging.InfoLevel)
//
//    producer.send(topic, msg).andThen {
//      case Success(status) =>
//        transactionId.finished(
//          this,
//          start,
//          s"posted to ${status.topic()}[${status.partition()}][${status.offset()}]",
//          logLevel = Logging.InfoLevel)
//      case Failure(_) => transactionId.failed(this, start, s"error on posting to topic $topic")
//    }
//  }
}

object EdgeBalancer extends LoadBalancerProvider {
  override def requiredProperties: Map[String, String] = kafkaHosts

  override def instance(whiskConfig: WhiskConfig, instance: ControllerInstanceId)(
    implicit actorSystem: ActorSystem,
    logging: Logging,
    materializer: ActorMaterializer): LoadBalancer = {
    val invokerPoolFactory = new InvokerPoolFactory {
      override def createInvokerPool(
        actorRefFactory: ActorRefFactory,
        messagingProvider: MessagingProvider,
        messageProducer: MessageProducer,
        sendActivationToInvoker: (MessageProducer, ActivationMessage, InvokerInstanceId) => Future[RecordMetadata],
        monitor: Option[ActorRef]): ActorRef = {

        InvokerPool.prepare(instance, WhiskEntityStore.datastore())

        actorRefFactory.actorOf(
          InvokerPool.props(
            (f, i) => f.actorOf(InvokerActor.props(i, instance)),
            (m, i) => sendActivationToInvoker(messageProducer, m, i),
            messagingProvider.getConsumer(whiskConfig, s"health${instance.asString}", "health", maxPeek = 128),
            monitor
          )
        )
      }
    }

    new EdgeBalancer(
      whiskConfig,
      instance,
      createFeedFactory(whiskConfig, instance),
      invokerPoolFactory)
  }

  def schedule(user: Namespace,
               action: FullyQualifiedEntityName,
               edgeBalancerState: EdgeBalancerState)(implicit random: Random): Option[ContainerInfo] = {
    edgeBalancerState.getContainers(user, action).flatMap { containers =>
      val weights = containers.map(_.weight)
      weightedRandomChoice(containers.zip(weights))
    }
  }

  private def weightedRandomChoice[T](weightedItems: IndexedSeq[(T, Int)])(implicit random: Random): Option[T] = {
    if (weightedItems.nonEmpty) {
      val weights = weightedItems.map(_._2)
      require(weights.forall(_ >= 0), new IllegalArgumentException("Weights must be non-negative"))

      val weightSum = weights.sum
      if (weightSum != 0) {
        val randomValue = random.nextInt(weightSum)
        val choiceIndex = weights.scanLeft(0)(_ + _).sliding(2).zipWithIndex.collectFirst {
          case Tuple2(IndexedSeq(start, end), index) if start until end contains randomValue => index
        }

        assert(choiceIndex.isDefined)

        choiceIndex.map(index => weightedItems(index)._1)
      } else {
        // Non-empty items but all the weights are 0
        None
      }
    } else {
      // Empty items
      None
    }
  }
}

trait EdgeBalancerState {
  def invokers: IndexedSeq[InvokerHealth]

  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit

  def getContainers(user: Namespace, action: FullyQualifiedEntityName): Option[IndexedSeq[ContainerInfo]]

  def compareAndSetContainers(user: Namespace,
                              action: FullyQualifiedEntityName,
                              expected: IndexedSeq[ContainerInfo],
                              newContainers: IndexedSeq[ContainerInfo]): Boolean

  def putIfAbsent(user: Namespace,
                  action: FullyQualifiedEntityName,
                  newContainers: IndexedSeq[ContainerInfo]): Boolean
}

object EdgeBalancerStateSingleton extends EdgeBalancerState {
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty

  private val _containersPerAction: collection.concurrent.Map[(Namespace, FullyQualifiedEntityName), IndexedSeq[ContainerInfo]] =
    collection.concurrent.TrieMap.empty

  override def invokers: IndexedSeq[InvokerHealth] = _invokers

  override def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = this._invokers = newInvokers

  override def getContainers(user: Namespace, action: FullyQualifiedEntityName): Option[IndexedSeq[ContainerInfo]] =
    this._containersPerAction get (user, action)

  override def compareAndSetContainers(user: Namespace,
                                       action: FullyQualifiedEntityName,
                                       expected: IndexedSeq[ContainerInfo],
                                       newContainers: IndexedSeq[ContainerInfo]): Boolean = {
    this._containersPerAction.replace((user, action), expected, newContainers)
  }

  override def putIfAbsent(user: Namespace,
                           action: FullyQualifiedEntityName,
                           newContainers: IndexedSeq[ContainerInfo]): Boolean =
    this._containersPerAction.putIfAbsent((user, action), newContainers).isEmpty
}

case class ContainerInfo(
  id: String,
  invoker: InvokerInstanceId,
  memory: ByteSize,
  cpu: CpuTime) {
  def weight = cpu.milliCpus
}
