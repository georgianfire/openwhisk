package org.apache.openwhisk.core.containerpool.kubernetes

import akka.actor.{Actor, ActorRef, ActorRefFactory, Props}
import org.apache.openwhisk.common.{AkkaLogging, Logging, LoggingMarkers, MetricEmitter}
import org.apache.openwhisk.core.connector.ActivationMessage
import org.apache.openwhisk.core.containerpool.{ContainerId, ContainerRemoved, Remove, Run, RunWithSize}
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

  case class RunOnContainer(action: ExecutableWhiskAction,
                            msg: ActivationMessage,
                            containerId: ContainerId) extends ActionExecutionMessage

  case class RunOnNewContainerWithSize(action: ExecutableWhiskAction,
                                       msg: ActivationMessage,
                                       memory: ByteSize,
                                       cpu: CpuTime) extends ActionExecutionMessage
}

class EdgeContainerPool(containerFactory: ActorRefFactory => ActorRef) extends Actor {
  implicit val logging: Logging = new AkkaLogging(context.system.log)
  implicit val executionContext: ExecutionContext = context.dispatcher

  var pool = immutable.Map.empty[ContainerId, ActorRef]
  var containerDataByRef = immutable.Map.empty[ActorRef, EdgeContainerData]

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
          containerDataByRef = containerDataByRef.updated(ephemeralContainer, EdgeContainerData(isEphermal=true))

        case RunOnContainer(action, msg, containerId) =>
          assert(pool.contains(containerId))
          val container = pool(containerId)
          container ! Run(action, msg)

        case RunOnNewContainerWithSize(action, msg, memory, cpu) =>
          val container = containerFactory(context)
          container ! RunWithSize(action, msg, memory, cpu)
      }
    }

    case ContainerRemoved =>
      val containerData = containerDataByRef(sender())
      if (containerData.isEphermal) {
        logging.info(this, "Emphermal container removed.")
        containerDataByRef = containerDataByRef - sender()
      }
      else ???

    case unknownMessage => logging.error(this, s"Received unknown message: $unknownMessage")
  }

  def createContainer(memoryLimit: ByteSize, cpuTime: CpuTime) = ???

  private def emitMetrics(): Unit = {
    MetricEmitter.emitGaugeMetric(LoggingMarkers.CONTAINER_POOL_ACTIVE_COUNT, pool.size)
  }
}

object EdgeContainerPool {
  def props(factory: ActorRefFactory => ActorRef) = Props(new EdgeContainerPool(factory))
}
