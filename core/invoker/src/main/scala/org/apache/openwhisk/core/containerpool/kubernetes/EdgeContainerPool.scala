package org.apache.openwhisk.core.containerpool.kubernetes

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.{AkkaLogging, Logging, LoggingMarkers, MetricEmitter}
import org.apache.openwhisk.core.connector.{ActivationMessage, MessageFeed}
import org.apache.openwhisk.core.containerpool.{ContainerRemoved, Remove, Resize, Run, RunWithSize, Start}
import org.apache.openwhisk.core.entity.{ByteSize, CpuLimit, CpuTime, ExecutableWhiskAction, MemoryLimit}

import scala.collection.immutable
import scala.concurrent.ExecutionContext

sealed trait EdgeContainerOperationMessage

object EdgeContainerOperationMessage {
  case class CreateContainer() extends EdgeContainerOperationMessage

  case class DeleteContainer() extends EdgeContainerOperationMessage

  case class ResizeContainer() extends EdgeContainerOperationMessage

  case class GracefulTerminateContainer() extends EdgeContainerOperationMessage
}

sealed trait ActionExecutionMessage

case class EdgeContainerData(isEphermal: Boolean)

object ActionExecutionMessage {
  case class RunWithEphemeralContainer(action: ExecutableWhiskAction,
                                       msg: ActivationMessage) extends ActionExecutionMessage

  case class RunOnContainerWithSize(action: ExecutableWhiskAction,
                                       msg: ActivationMessage,
                                       containerId: String,
                                       memory: ByteSize,
                                       cpu: CpuTime) extends ActionExecutionMessage
}

class EdgeContainerPool(containerFactory: ActorRefFactory => ActorRef,
                        activationFeed: ActorRef) extends Actor {
  implicit val logging: Logging = new AkkaLogging(context.system.log)
  implicit val executionContext: ExecutionContext = context.dispatcher

  var pool = immutable.Map.empty[String, ActorRef]

  override def receive: Receive = {
    case op: EdgeContainerOperationMessage => {
      // Message Sent from resource manager
      import EdgeContainerOperationMessage._

      op match {
        case CreateContainer() => ???
        case DeleteContainer() => ???
        case ResizeContainer() => ???
        case GracefulTerminateContainer() => ???
      }
    }

    case msg: ActionExecutionMessage => {
      // Message sent from loadbalancer
      import ActionExecutionMessage._

      msg match{
        case RunWithEphemeralContainer(action, msg) =>
          val ephemeralContainer = containerFactory(context)
          ephemeralContainer ! RunWithSize(action, msg, MemoryLimit.MIN_MEMORY, CpuLimit.MIN_CPU)
          ephemeralContainer ! Remove

        case RunOnContainerWithSize(action, msg, containerId, memory, cpu) =>
          if (pool contains containerId) {
              logging.info(this, s"scheduling to container $containerId")
              val container = pool(containerId)
              container ! Run(action, msg)
          } else {
              logging.info(this, s"cold starting container $containerId")
              val container = containerFactory(context)
              pool = pool.updated(containerId, container)
              container ! Start(action.exec, memory)
              container ! Resize(None, Some(cpu))
              container ! Run(action, msg)
          }
      }

      activationFeed ! MessageFeed.Processed
    }

    case ContainerRemoved =>
      logging.info(this, "Container removed")

    case unknownMessage => logging.error(this, s"Received unknown message: $unknownMessage")
  }

  def createContainer(memoryLimit: ByteSize, cpuTime: CpuTime) = ???

  private def emitMetrics(): Unit = {
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_ACTIVE_COUNT, pool.size)
  }
}

object EdgeContainerPool {
  def props(factory: ActorRefFactory => ActorRef, activationFeed: ActorRef) = Props(new EdgeContainerPool(factory, activationFeed))
}
