package org.apache.openwhisk.core.loadBalancer

import akka.actor.ActorSystem
import akka.stream.ActorMaterializer
import org.apache.openwhisk.common.{Logging, TransactionId}
import org.apache.openwhisk.core.WhiskConfig
import org.apache.openwhisk.core.connector.{ActivationMessage, MessagingProvider}
import org.apache.openwhisk.core.entity.{ActivationId, ControllerInstanceId, ExecutableWhiskActionMetaData, InvokerInstanceId, WhiskActivation}
import org.apache.openwhisk.spi.SpiLoader

import scala.concurrent.Future

class EdgeBalancer(
  config: WhiskConfig,
  controllerInstance: ControllerInstanceId,
  feedFactory: FeedFactory,
  implicit val messagingProvider: MessagingProvider = SpiLoader.get[MessagingProvider])(
  implicit actorSystem: ActorSystem,
  logging: Logging,
  materializer: ActorMaterializer)
    extends CommonLoadBalancer(config, feedFactory, controllerInstance) {

  override val invokerPool = ???

  override def releaseInvoker(invoker: InvokerInstanceId, entry: ActivationEntry): Unit = ???

  override def invokerHealth(): Future[IndexedSeq[InvokerHealth]] = ???

  override def publish(action: ExecutableWhiskActionMetaData, msg: ActivationMessage)(implicit transid: TransactionId): Future[Future[Either[ActivationId, WhiskActivation]]] = ???
}
