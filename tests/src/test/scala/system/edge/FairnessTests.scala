package system.edge

import java.time.Instant

import common.rest.{RestResult, WskRestOperations}
import common.{RunCliCmd, TestHelpers, TestUtils, WskActorSystem, WskAdmin, WskTestHelpers}
import org.apache.openwhisk.core.containerpool.Interval
import org.junit.runner.RunWith
import org.scalatest.BeforeAndAfterAll
import org.scalatestplus.junit.JUnitRunner
import org.apache.openwhisk.core.entity.CpuTime
import org.apache.openwhisk.core.entity.size._

import scala.concurrent.{Await, Future}
import scala.concurrent.duration.Duration

@RunWith(classOf[JUnitRunner])
class FairnessTests  extends TestHelpers with WskTestHelpers with WskActorSystem with BeforeAndAfterAll {
  val wskOperations: WskRestOperations = new WskRestOperations
  implicit val wskadmin: RunCliCmd = WskAdmin.kubeWskadmin
  val user1 = getAdditionalTestSubject("user1")
  val user2 = getAdditionalTestSubject("user2")


  override def afterAll() = {
    disposeAdditionalTestSubject(user1.namespace)
    disposeAdditionalTestSubject(user2.namespace)
  }

  wskadmin.cli(
    Seq("limits", "set", user1.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "8"))

  wskadmin.cli(
    Seq("limits", "set", user2.namespace,
      "--invocationsPerMinute", "102400",
      "--concurrentInvocations", "102400",
      "--firesPerMinute", "102400",
      "--weight", "8"))


  it should "create function 1" in {
    wskOperations.action.create(
      "prime",
      Some(TestUtils.getTestActionFilename("prime.py")),
      memory = Some(256.MB),
      cpu = Some(CpuTime(400)))(user1)

    wskOperations.action.create(
      "mnv3",
      None,
      docker = Some("binw/mnv3-action"),
      memory = Some(512.MB),
      cpu = Some(CpuTime(1000)))(user2)

    val start = Instant.now


    val f1 = Future {
      var futures: Seq[Future[RestResult]] = Seq.empty
      while (Interval(start, Instant.now).duration.toMinutes < 10) {
        futures = futures :+ Future {
          val invocationResult = wskOperations.action.invoke("prime")(user1)
          val activationId = wskOperations.activation.extractActivationId(invocationResult)
          wskOperations.activation.get(activationId)(user2)
        }
        Thread.sleep(100)
      }
      Await.ready(Future.sequence(futures), Duration.Inf)
    }

    val f2 = Future {
      var futures: Seq[Future[RestResult]] = Seq.empty
      while (Interval(start, Instant.now).duration.toMinutes < 10) {
        futures = futures :+ Future {
          val invocationResult = wskOperations.action.invoke("mnv3")(user2)
          val activationId = wskOperations.activation.extractActivationId(invocationResult)
          wskOperations.activation.get(activationId)(user2)
        }
        Thread.sleep(500)
      }
      Await.result(Future.sequence(futures), Duration.Inf)
    }

    val futures = Future.sequence(Seq(f1, f2))
    Await.result(futures, Duration.Inf)
  }
}
