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
import spray.json.JsNumber

import scala.concurrent.Future
import scala.util.Random

@RunWith(classOf[JUnitRunner])
class QueueingTests extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll{
  implicit val wskadmin = WskAdmin.kubeWskadmin
  val wskOperations: WskRestOperations = new WskRestOperations
  val name = "prime"
  implicit val user = getAdditionalTestSubject(name)

  override def afterAll(): Unit = {
    disposeAdditionalTestSubject(user.namespace)
  }

  wskadmin.cli(
    Seq("limits", "set", user.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400"))

  val highRate = 30
  val lowRate = 5
  implicit val rng = new Random()

  def getRate(start: Instant, now: Instant):Double = {
    Interval(start, now).duration.toMinutes + 1
  }

  def genExponential(lambda: Double)(implicit rng: Random): Double = {
    math.log(1 - rng.nextDouble()) / (-lambda)
  }

  it should "Queueing model" in withAssetCleaner(user) { (wp, assetHelper) =>
    assetHelper.withCleaner(wskOperations.action, name) { (action, _) =>
      action.create(name, Some(TestUtils.getTestActionFilename("prime.py")), memory = Some(256.MB), cpu = Some(CpuTime(500)))
    }

    val start = Instant.now
    println(start)


    while (Interval(start, Instant.now).duration.toMinutes < 5) {
      val activationId = Future{
        val invokeResult = wskOperations.action.invoke(name, parameters = Map("n" -> JsNumber(5000)))
        val activationId = wskOperations.activation.extractActivationId(invokeResult).get
        wskOperations.activation.waitForActivation(activationId) match {
          case Left(value) => println(value)
          case Right(value) => println(value)
        }
      }

      Thread.sleep((genExponential(getRate(start, Instant.now())) * 1000).toInt)
    }

  }
}

