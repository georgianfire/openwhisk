package org.apache.openwhisk.core.invoker

import java.nio.charset.StandardCharsets

import akka.actor.{ActorRefFactory, ActorSystem, Props}
import akka.event.Logging.InfoLevel
import org.apache.openwhisk.common.tracing.WhiskTracerProvider
import org.apache.openwhisk.common.{Logging, LoggingMarkers, TransactionId}
import org.apache.openwhisk.core.connector.{ActivationMessage, CombinedCompletionAndResultMessage, ContainerOperationMessage, MessageFeed, MessageProducer}
import org.apache.openwhisk.core.containerpool.kubernetes.ActionExecutionMessage.{RunOnContainerWithSize, RunWithEphemeralContainer}
import org.apache.openwhisk.core.containerpool.kubernetes.EdgeContainerOperationMessage.CreateContainer
import org.apache.openwhisk.core.containerpool.kubernetes.EdgeContainerPool
import org.apache.openwhisk.core.containerpool.{ContainerPoolConfig, ContainerProxy}
import org.apache.openwhisk.core.database.{DocumentTypeMismatchException, DocumentUnreadable, NoDocumentException, UserContext}
import org.apache.openwhisk.core.entity.{ActivationResponse, ConcurrencyLimitConfig, DocRevision, FullyQualifiedEntityName, InvokerInstanceId, TimeLimit, WhiskAction}
import org.apache.openwhisk.core.{ConfigKeys, WhiskConfig}
import org.apache.openwhisk.http.Messages
import pureconfig._
import pureconfig.generic.auto._

import scala.concurrent.Future
import scala.concurrent.duration._

class EdgeInvoker(whiskConfig: WhiskConfig,
                  instance: InvokerInstanceId,
                  producer: MessageProducer,
                  poolConfig: ContainerPoolConfig = loadConfigOrThrow[ContainerPoolConfig](ConfigKeys.containerPool),
                  limitConfig: ConcurrencyLimitConfig = loadConfigOrThrow[ConcurrencyLimitConfig](
                    ConfigKeys.concurrencyLimit))(implicit actorSystem: ActorSystem, logging: Logging)
    extends InvokerReactive(whiskConfig, instance, producer, poolConfig, limitConfig) {

  logging.info(this, s"Initializing $getClass")

  private val systemNamespace: String = "whisk.system"

  private val controlTopic = s"invoker${instance.toInt}-control"
  private val controlMessageConsumer =
    msgProvider.getConsumer(whiskConfig, topic, controlTopic, maxPeek, maxPollInterval = TimeLimit.MAX_DURATION + 1.minute)
  private val controlFeed = actorSystem.actorOf(Props(
    new MessageFeed("control", logging, controlMessageConsumer, maxPeek, 1.second, processControlMessage)
  ))

  private val childFactory = (f: ActorRefFactory) =>
    f.actorOf(ContainerProxy.props(containerFactory.createContainer,
      ack,
      store,
      collectLogs,
      instance,
      poolConfig,
      factoryWithFixedSize = Some(containerFactory.createContainerWithFixedSize)))

  private val pool = actorSystem.actorOf(EdgeContainerPool.props(childFactory, activationFeed))

  override def processActivationMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ActivationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg: ActivationMessage =>
        implicit val transid: TransactionId = msg.transid

        logging.info(this, s"${msg.user.namespace.name} ${msg.containerId}")

        // Set trace context to continue tracing
        WhiskTracerProvider.tracer.setTraceContext(transid, msg.traceContext)

        if (!namespaceBlacklist.isBlacklisted(msg.user)) {
          // Note: a potential pitfall is that the user and action in the same activation can belong to different namespace
          // This should be avoided in experiment by users only running actions in its own namespace
          // TODO: fix this problem later
          transid.started(this, LoggingMarkers.INVOKER_ACTIVATION, logLevel = InfoLevel)
          val namespace = msg.action.path
          val name = msg.action.name
          val actionId = FullyQualifiedEntityName(namespace, name).toDocId.asDocInfo(msg.revision)
          val subject = msg.user.subject

          val hasRevision: Boolean = actionId.rev != DocRevision.empty
          if (!hasRevision) logging.warn(this, s"revision was not provided for ${actionId.id}")

          WhiskAction
            .get(entityStore, actionId.id, actionId.rev, fromCache = hasRevision)
            .flatMap { action =>
              action.toExecutableWhiskAction match {
                case Some(executable) =>
                  // This is a user action and the target container should be specified
                  if (msg.user.namespace.name.asString != systemNamespace) {
                    val containerId = msg.containerId.get
                    val (memory, cpu) = msg.size.get
                    pool ! RunOnContainerWithSize(executable, msg, containerId, memory, cpu)
                  } else {
                    pool ! RunWithEphemeralContainer(executable, msg)
                  }
                  Future.successful(())
                case None =>
                  logging.error(this, s"non-executable action reached the invoker: ${action.fullyQualifiedName(false)}")
                  Future.failed(new IllegalArgumentException("non-executable action reached the invoker"))
              }
            }
            .recoverWith {
              case t =>
                val response = t match {
                  case _: NoDocumentException =>
                    ActivationResponse.applicationError(Messages.actionRemovedWhileInvoking)
                  case _: DocumentTypeMismatchException | _: DocumentUnreadable =>
                    ActivationResponse.whiskError(Messages.actionMismatchWhileInvoking)
                  case _ =>
                    ActivationResponse.whiskError(Messages.actionFetchErrorWhileInvoking)
                }
                activationFeed ! MessageFeed.Processed

                val activation = generateFallbackActivation(msg, response)
                ack(
                  msg.transid,
                  activation,
                  msg.blocking,
                  msg.rootControllerIndex,
                  msg.user.namespace.uuid,
                  CombinedCompletionAndResultMessage(transid, activation, instance))

                store(msg.transid, activation, msg.blocking, UserContext(msg.user))
                Future.successful(())
            }
        } else {
          activationFeed ! MessageFeed.Processed

          val activation =
            generateFallbackActivation(msg, ActivationResponse.applicationError(Messages.namespacesBlacklisted))

          ack(
            msg.transid,
            activation,
            msg.blocking,
            msg.rootControllerIndex,
            msg.user.namespace.uuid,
            CombinedCompletionAndResultMessage(transid, activation, instance))

          logging.warn(this, s"namespace ${msg.user.namespace.name} was blocked.")
          Future.successful(())
        }
      }
      .recoverWith {
        case t =>
          // Iff everything above failed, we have a terminal error at hand. Either the message failed
          // to deserialize, or something threw an error where it is not expected to throw.
          activationFeed ! MessageFeed.Processed
          logging.error(this, s"terminal failure while processing message: $t")
          Future.successful(())
      }
  }

  def processControlMessage(bytes: Array[Byte]): Future[Unit] = {
    Future(ContainerOperationMessage.parse(new String(bytes, StandardCharsets.UTF_8)))
      .flatMap(Future.fromTry)
      .flatMap { msg: ContainerOperationMessage =>
        implicit val transactionId = msg.transid
        import ContainerOperationMessage._
        logging.info(this, s"Received ContainerOperationMessage: $msg")
        msg.operation match {
          case Create =>
            val action = msg.action.get
            val rev = DocRevision("0.0.1")
            val actionId = action.toDocId.asDocInfo(rev)
            WhiskAction
              .get(entityStore, actionId.id, actionId.rev, fromCache = true)
              .flatMap { action =>
                action.toExecutableWhiskAction match {
                  case Some(executable) =>
                    val containerId = msg.containerId
                    val memory = msg.memory.get
                    val cpu = msg.cpu.get

                    logging.info(this, s"Creating container for action $actionId with $memory memory and $cpu cpu")
                    pool ! CreateContainer(executable, containerId, memory, cpu)
                    Future.successful(())
                  case None =>
                    logging.error(this, s"non-executable action reached the invoker: ${action.fullyQualifiedName(false)}")
                    Future.failed(new IllegalArgumentException("non-executable action reached the invoker"))
                }
              }

          case Resize => ???
          case ForceTerminate => ???
          case GracefullyTerminate => ???
        }
      }
  }
}

object EdgeInvoker extends InvokerProvider {
  override def instance(
    config: WhiskConfig,
    instance: InvokerInstanceId,
    producer: MessageProducer,
    poolConfig: ContainerPoolConfig,
    limitConfig: ConcurrencyLimitConfig)(implicit actorSystem: ActorSystem, logging: Logging): InvokerCore =
    new EdgeInvoker(config, instance, producer, poolConfig, limitConfig)
}
