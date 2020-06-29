package system.edge

import java.time.Instant

import org.apache.openwhisk.core.entity.CpuTime
import org.apache.openwhisk.core.entity.size._
import common.rest.WskRestOperations
import common.{TestHelpers, TestUtils, WskActorSystem, WskAdmin, WskTestHelpers}
import org.apache.openwhisk.core.containerpool.Interval
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import spray.json.JsObject

import scala.collection.mutable
import scala.concurrent.{Await, Future}
import scala.concurrent.duration._
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class QueueingTests extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll{
  implicit val wskadmin = WskAdmin.kubeWskadmin
  val wskOperations: WskRestOperations = new WskRestOperations
  val name = "prime"
  implicit val user = getAdditionalTestSubject(name)

  wskadmin.cli(
    Seq("limits", "set", user.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400"))

  override def afterAll(): Unit = {
    disposeAdditionalTestSubject(user.namespace)
  }

  val highRate = 30
  val lowRate = 5
  implicit val rng = new Random()

  def genExponential(lambda: Double)(implicit rng: Random): Double = {
    math.log(1 - rng.nextDouble()) / (-lambda)
  }

  it should "Queueing model" in withAssetCleaner(user) { (wp, assetHelper) =>
    assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("prime.py")), memory = Some(256.MB), cpu = Some(CpuTime(500)))
    }

    val start = Instant.now
    val activationIds = mutable.Buffer[Future[String]]()

    while (Interval(start, Instant.now).duration.toMinutes < 5) {
      val invokeResult = wskOperations.action.invoke(name)
      val activationId = Future{ wskOperations.activation.extractActivationId(invokeResult).get }
      activationIds += activationId
      Thread.sleep((genExponential(lowRate) * 1000).toInt)
    }

    val activationResultsFuture: Future[Seq[Either[String, JsObject]]] =
      Future.sequence(activationIds.map( activationId => {
        activationId.map(wskOperations.activation waitForActivation _)
      }))

    val activationResults = Await.ready(activationResultsFuture, 5.minutes).value.get
    activationResults.foreach( results => results.foreach{
      case Left(value) => println(value)
      case Right(value) => println(value.compactPrint)
    })
  }
}

