package org.apache.openwhisk.core.loadBalancer

import akka.actor.{Actor, ActorRef, ActorRefFactory, ActorSystem, Props}
import akka.stream.ActorMaterializer
import org.apache.kafka.clients.producer.RecordMetadata
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.WhiskConfig._
import org.apache.openwhisk.core.connector.{ActivationMessage, MessageProducer, MessagingProvider}
import org.apache.openwhisk.core.containerpool.ContainerId
import org.apache.openwhisk.core.entity._
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

  val schedulingState: EdgeBalancerState = EdgeBalancerState()

  private val monitor = actorSystem.actorOf(Props(new Actor{
    override def receive: Receive = {
      case CurrentInvokerPoolState(newState) => schedulingState.updateInvokers(newState)

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

  override def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = ???

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = Future.successful(schedulingState.invokers)

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)
                      (implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = {
    EdgeBalancer.schedule(msg.action, schedulingState).map{ containerInfo =>
      val invoker = containerInfo.invoker
      val msgWithContainerId = msg.copy(containerId = Some(containerInfo.id))
      val activationResult = setupActivation(msgWithContainerId, action, invoker)
      sendActivationToInvoker(messageProducer, msgWithContainerId, invoker).map(_ => activationResult)
    }.getOrElse {
      // TODO: This should be due to the action has no running containers.
      // Create one container and schedule to it.
      ???
    }
  }
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

  def schedule(action: FullyQualifiedEntityName,
               edgeBalancerState: EdgeBalancerState)(implicit random: Random): Option[ContainerInfo] = {
    edgeBalancerState.getContainers(action).flatMap { containers =>
      val availableContainers = containers.filter(_.state == ContainerState.Running)
      val weights = availableContainers.map(_.weight)
      weightedRandomChoice(availableContainers.zip(weights))
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

case class EdgeBalancerState(
  private var _invokers: IndexedSeq[InvokerHealth] = IndexedSeq.empty[InvokerHealth]) {

  private var _containersPerAction: Map[FullyQualifiedEntityName, IndexedSeq[ContainerInfo]] = Map.empty

  def invokers: IndexedSeq[InvokerHealth] = _invokers

  def getContainers(action: FullyQualifiedEntityName): Option[IndexedSeq[ContainerInfo]] = {
    _containersPerAction.get(action)
  }

  def updateInvokers(newInvokers: IndexedSeq[InvokerHealth]): Unit = {
    _invokers = newInvokers
  }

  def updateContainers(action: FullyQualifiedEntityName, containers: IndexedSeq[ContainerInfo]): Unit = {
    _containersPerAction = _containersPerAction.updated(action, containers)
  }
}

sealed trait ContainerState

object ContainerState {
  object Running extends ContainerState

  object Terminating extends ContainerState
}

case class ContainerInfo(
  id: ContainerId,
  invoker: InvokerInstanceId,
  state: ContainerState,
  weight: Int)
